import Tkinter
import ScrolledText
import sys
import threading
import os
import fcntl
import select
import socket
from collections import deque
from wootlite import WootNote


class IOMonitor(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.name = "monitor"
        # socket/fd->interface implementing onRead/onWrite/onError
        self.readers = dict()
        self.writers = dict()
        self.lock = threading.Lock()
        self.readersToRemove = set()
        self.writersToRemove = set()
        self.done = False
        r, w = os.pipe()
        self.intpipe = (os.fdopen(r), os.fdopen(w, 'w'))
        fcntl.fcntl(self.intpipe[0], fcntl.F_SETFL, os.O_NONBLOCK)

    def interrupt(self, what='i'):
        self.intpipe[1].write(what)
        self.intpipe[1].flush()

    def addReader(self, r):
        '''
        In Python 2.5 and later, you can also use the with statement.
        When used with a lock, this statement automatically
        acquires the lock before entering the block, and releases
        it when leaving the block:
        '''
        with self.lock:
            self.readers[r.fileno()] = r
        self.interrupt()

    def addWriter(self, w):
        with self.lock:
            # sys.stderr.write("add writer %d\n" % (w.fileno()))
            self.writers[w.fileno()] = w
        self.interrupt()

    def removeReader(self, r):
        with self.lock:
            self.readersToRemove.add(r.fileno())
        self.interrupt()

    def removeWriter(self, w):
        with self.lock:
            # sys.stderr.write("remove writer %d\n" % (w.fileno()))
            self.writersToRemove.add(w.fileno())
        self.interrupt()

    def requestExit(self):
        with self.lock:
            self.done = True
        # print "write q to pipe " + str(self.intpipe[1].fileno())
        self.interrupt('q')
        self.join()

    def exitPending(self):
        with self.lock:
            return self.done

    def discontinueMonitoring(self, toRemove, fromDict):
            # remove any to be closed readers/writers
            removedList = []
            # print toRemove, fromDict
            with self.lock:
                while len(toRemove):
                    t = toRemove.pop()
                    removedList.append(fromDict[t])
                    del fromDict[t]
            [e.onExit() for e in removedList]

    def run(self):
        while not self.exitPending():
            # generate list of readers and writes
            with self.lock:
                readers = [rf for rf in self.readers.keys()]
                writers = [wf for wf in self.writers.keys()]
                # print "append quit fileno " + str(self.intpipe[0].fileno())
                readers.append(self.intpipe[0].fileno())
                # print readers

            # print writers
            # without mLock
            # print "wait in select"
            rList, wList, xList = select.select(
                readers,
                writers,
                [e for e in set(readers).union(writers)])
            # print "out of select"
            for x in xList:
                if x in self.readers:
                    self.readers[x].onError()
                elif x in self.writers:
                    self.writers[x].onError()
            # print "check int pipe"
            if self.intpipe[0].fileno() in rList:
                w = self.intpipe[0].read(1)
                # print "got %s from int pipe" % (w)
                if w[0] == 'q':
                    # print 'got quit'
                    break
                elif w[0] == 'i':
                    # print 'got int'
                    pass

            for r in rList:
                if r != self.intpipe[0].fileno():
                    self.readers[r].onRead()

            for w in wList:
                self.writers[w].onWrite()

            # remove any readers/writes requested while we were
            # in a read/write
            self.discontinueMonitoring(self.readersToRemove, self.readers)
            self.discontinueMonitoring(self.writersToRemove, self.writers)
            self.readersToRemove = set()
            self.writersToRemove = set()

        self.discontinueMonitoring(set(self.readers.keys()), self.readers)
        self.discontinueMonitoring(set(self.writers.keys()), self.writers)


class SimpleNotepad:

    class ConnectionListener:
        def mkSocket(self, host, port):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.setblocking(0)
            s.bind((host, port))
            s.listen(1)
            return s

        def __init__(self, sn):
            global sIOMonitor
            self.socket = self.mkSocket(sn.host, sn.port)
            self.sn = sn
            sIOMonitor.addReader(self)

        def fileno(self):
            return self.socket.fileno()

        def onRead(self):
            sys.stderr.write("wait on accept\n")
            (clientSocket, address) = self.socket.accept()
            sys.stderr.write("got connection %s\n" % (str(address)))
            self.sn.onConnected(clientSocket)

        def onError(self, msg):
            sys.stderr.write('ConnectionListener: %s\n' % (msg))

        def onExit(self):
            self.socket.close()
            self.sn.onDisconnected()

    class ClientReadMonitor:
        def __init__(self, sn, socket):
            global sIOMonitor
            self.sn = sn
            self.socket = socket
            sIOMonitor.addReader(self)

        def fileno(self):
            return self.socket.fileno()

        def onRead(self):
            self.sn.onRecvFrom(self.socket)

        def onError(self):
            global sIOMonitor
            sys.stderr.write(
                "readmonitor: got error from socket %d\n" % (self.fileno()))
            sIOMonitor.removeReader(self)

        def onExit(self):
            pass

    class ClientWriteMonitor:
        def __init__(self, sn, socket):
            self.sn = sn
            self.socket = socket
            self.resume()

        def pause(self):
            global sIOMonitor
            sIOMonitor.removeWriter(self)

        def resume(self):
            global sIOMonitor
            sIOMonitor.addWriter(self)

        def fileno(self):
            return self.socket.fileno()

        def onWrite(self):
            self.sn.onSendTo(self.socket)

        def onError(self):
            sys.stderr.write(
                "writemonitor: got error from socket %d\n", self.fileno())
            global sIOMonitor
            sIOMonitor.removeWriter(self)

        def onExit(self):
            pass

    def readUpdates(self):
        # This should be updated to a set
        # We do not check for dependency preservation
        # as thats assumed in a 2 party system with
        # FIFO communication
        op_count = 0
        while len(self.readq) > 0:
            op_str = self.readq.popleft()
            if op_str[0] == 'I':
                char, pos = self.wootNote.remoteIns(op_str)
                self.window.insert("1.0+%dc" % (pos), char)
            elif op_str[0] == 'D':
                pos = self.wootNote.remoteDel(op_str)
                self.window.delete("1.0+%dc" % (pos))
            op_count += 1
        return op_count

    def broadcast(self, op):
        was_empty = len(self.writeq) == 0
        self.writeq.append(op+'\n')
        if self.remote['writer'] is None:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.otherhost, self.otherport))
            # print "connect done"
            with self.lock:
                self.remote['writer'] = self.ClientWriteMonitor(self, s)
                # sys.stderr.write(
                # "onIdle writer %s\n" % (hex(id(self.remote['writer']))))
        elif was_empty:
            self.remote['writer'].resume()

    def onReadUpdates(self):
        # overlay latest read change
        if self.readUpdates() > 0:
            self.window.edit_modified(False)
        if self.checkIdleId is not None:
            self.window.after_cancel(self.checkIdleId)
        self.schedReadUpdate()

    def schedReadUpdate(self):
        if self.window.edit_modified():
            self.window.after_idle(lambda x: x.onReadUpdates(), self)
        else:
            self.checkIdleId = self.window.after(
                        200,
                        lambda x: x.onReadUpdates(), self)

    def onKey(self, e):
        """
        cb is issued before the key is handled. interpretation
        depends on the key pressed

        At the generate site, onKey() is used
        to translate each key into an integrate
        and broadcast. we always return break
        as on each key, the index of insert may not
        be the INSERT index. Index may not be INSERT
        index if remote updates are also processed
        in this context

        When integrating remote operations.
        we can opt to do it in onIdle().
        The reason being the Cp and Cn when
        integrating are already known when
        the op was generated. Doing in onIdle
        guarantees that the doc state isn't
        changing and is good to use.

        Another option is to integrate remote operations from
        onReadFrom but this may be risky as the
        doc state is being edited concurrently
        """
        pos = len(self.window.get("1.0", self.window.index(Tkinter.INSERT)))
        if e.keysym == "Delete":
            if self.window.tag_ranges("sel"):
                """
                if a selection is active and delete is pressed, then
                the chars between sel_first and sel_last are deleted.
                del l1.c1 -> l2.c2 deletes all (visible) chars from the
                c1th visible char of l1 to the c2th visible char of l2
                """
                first_pos = len(self.window.get(
                    "1.0", self.window.index(Tkinter.SEL_FIRST)))
                last_pos = len(self.window.get(
                    "1.0", self.window.index(Tkinter.SEL_LAST)))-1
                # sys.stderr.write("Del Sel (%d, %d)\n" % (first_pos, last_pos))
                for pos in range(first_pos, last_pos+1):
                    self.broadcast(self.wootNote.generateDel(pos))
            else:
                """
                there is no selection. in this case, the cursor is
                between l1.c1-1 and l1.c1
                the char to be deleted is l1.c1-1. Similar logic
                applies to backspace as well
                """
                # sys.stderr.write(
                #    "Del %s pos %d\n" %
                #    (self.window.index(Tkinter.INSERT), pos))
                self.broadcast(self.wootNote.generateDel(pos))
        elif e.keysym == "BackSpace":
            if pos >= 1:
                # sys.stderr.write(
                #    "Bspc %s will del at pos %d\n" %
                #    (self.window.index(Tkinter.INSERT), pos-1))
                self.broadcast(self.wootNote.generateDel(pos-1))
        elif e.keysym == "Return":
            # sys.stderr.write("Ret %s pos %d\n" %
            #                 (self.window.index(Tkinter.INSERT), pos))
            self.broadcast(self.wootNote.generateIns(pos, '\n'))
        elif e.char != '' and e.keysym == e.char:
            # sys.stderr.write("K %s pos %d\n" %
            #                 (self.window.index(Tkinter.INSERT), pos))
            self.broadcast(self.wootNote.generateIns(pos, e.char))
        elif e.char == ' ':
            # sys.stderr.write("%s %s pos %d\n" %
            #                 (e.keysym,
            #                  self.window.index(Tkinter.INSERT), pos))
            self.broadcast(self.wootNote.generateIns(pos, e.char))
        else:
            pass

    def onDelete(self, e):
        print "after D:", e, self.window.index(Tkinter.INSERT)

    def onBackspace(self, e):
        print "after B:", e, self.window.index(Tkinter.INSERT)

    def onCopy(self, e):
        if self.window.tag_ranges("sel"):
            self.window.clipboard_clear()
            text = self.window.get(Tkinter.SEL_FIRST, Tkinter.SEL_LAST)
            sys.stderr.write("copy %s\n" % (text))
            self.window.clipboard_append(text)

    def onCut(self, e):
        if self.window.tag_ranges("sel"):
            self.onCopy(e)
            first_pos = len(self.window.get(
                "1.0",
                self.window.index(Tkinter.SEL_FIRST)))
            last_pos = len(self.window.get(
                "1.0",
                self.window.index(Tkinter.SEL_LAST)))
            for pos in range(first_pos, last_pos+1):
                self.broadcast(self.wootNote.generateDel(pos))
            self.window.delete(Tkinter.SEL_FIRST, Tkinter.SEL_LAST)
            # break to prevent Tkinter from cutting content as well
            return "break"

    def onPaste(self, e):
        text = self.window.selection_get(selection='CLIPBOARD')
        if text != '':
            sys.stderr.write("paste %s\n" % (text))
            pos = len(self.window.get("1.0",
                                      self.window.index(Tkinter.INSERT)))
            for i, char in enumerate(text):
                self.broadcast(self.wootNote.generateIns(pos+i, char))
            self.window.insert(Tkinter.INSERT, text)
            # break to prevent Tkinter from pasting content as well
            return "break"

    def onUndo(self, e):
        pass

    def onRedo(self, e):
        pass

    def __init__(self, host, port, otherhost, otherport, site, **kwargs):
        self.port = port
        self.host = host
        self.otherhost = otherhost
        self.otherport = otherport
        self.site = site
        self.root = Tkinter.Tk()
        self.frame = Tkinter.Frame(self.root, bg='grey')
        self.frame.pack(fill='both', expand='yes')
        self.window = ScrolledText.ScrolledText(master=self.frame,
                                                wrap='word',
                                                width=60,
                                                height=20,
                                                undo=True,
                                                autoseparators=True)
        self.window.pack(fill='both', expand=True, padx=8, pady=8)
        self.checkIdleId = None
        self.window.bind('<Key>', lambda e: self.onKey(e))

        def disable(e): return "break"

        def ignore(e): pass

        # TBD
        def undoredo(e):
            print e.keysym
            self.window.event_generate('<<UNDO>>' if
                                       e.keysym == '<Control-z>' else
                                       '<<REDO>>')
        map(lambda k: self.window.bind(k, disable), ['<Control-z>',
                                                     '<Shift-Control-z>'])
        map(lambda k: self.window.bind(k, ignore), ['<Escape>'])
        self.window.bind('<Control-c>', lambda e: self.onCopy(e))
        self.window.bind('<Control-x>', lambda e: self.onCut(e))
        self.window.bind('<Control-v>', lambda e: self.onPaste(e))
        self.readq = deque()
        self.writeq = deque()
        self.remote = {'reader': None, 'writer': None}
        self.connection = self.ConnectionListener(self)
        self.lock = threading.Lock()
        self.wootNote = WootNote(self.site, 0)
        self.schedReadUpdate()
        # print "__init__"

    def onConnected(self, socket):
        self.remote['reader'] = self.ClientReadMonitor(self, socket)
        return

    def onDisconnected(self):
        pass

    def onRecvFrom(self, socket):
        assert(socket == self.remote['reader'].socket)
        r = socket.recv(1024)
        if len(r) == 0:
            # print "got 0 bytes, remove"
            self.remote['reader'].onError()
        else:
            # sys.stderr.write('got %s\n' % (r, len(r)))
            self.readq.append(r)
            # self.schedReadUpdate()

    def onSendTo(self, socket):
        with self.lock:
            if self.remote['writer'] is None:
                sys.stderr.write("???\n")
                assert(0)
                return
        assert(socket == self.remote['writer'].socket)
        assert(len(self.writeq) > 0)
        # temp reference
        w = self.writeq[0]
        # sys.stderr.write("send: %s\n" % (w))
        bytes_sent = socket.send(w)
        if bytes_sent == len(w):
            self.writeq.popleft()
        if len(self.writeq) == 0:
            self.remote['writer'].pause()

    def run(self):
        # Run main application
        self.root.mainloop()


if __name__ == "__main__":
    import traceback
    if len(sys.argv) < 6:
        print "sn host port otherhost otherport site"
    else:
        sIOMonitor = IOMonitor()
        sIOMonitor.start()
        try:
            sn = SimpleNotepad(sys.argv[1],
                               int(sys.argv[2]),
                               sys.argv[3],
                               int(sys.argv[4]),
                               int(sys.argv[5]))
            sn.run()
        except:
            # print "starting notepad failed"
            traceback.print_exc(file=sys.stdout)
        sIOMonitor.requestExit()
        sys.stderr.write("all done\n")
