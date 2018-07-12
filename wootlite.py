import sys
import threading
import time
import random
from collections import deque


class WChar:
    def __init__(self, identifier, visible, alpha, prev_id, next_id):
        self.identifier = identifier
        self.visible = visible
        # will have to use characters.
        # with words ins space => word wchar must be split
        # del space => word wchar must be merged
        # probably an optimization
        self.alpha = alpha
        self.id_cp = prev_id
        self.id_cn = next_id

    # override lt to provide id check
    def __lt__(self, other):
        if self.identifier[0] != other.identifier[0]:
            return self.identifier[0] < other.identifier[0]
        else:
            return self.identifier[1] < other.identifier[1]

    def __eq__(self, other):
        return self.identifier == other.identifier

    def __str__(self):
        return ', '.join(str(x) for x in [self.identifier,
                                          self.visible,
                                          self.alpha,
                                          self.id_cp,
                                          self.id_cn])


kBegin = WChar((-1, 0), True, 'Cb', None, (32768, 0))
kEnd = WChar((32768, 0), True, 'Ce', (-1, 0), None)
kNone = WChar(None, False, '', None, None)


class WString:

    def __init__(self):
        self.wchars = [kBegin, kEnd]
        self.count = [2]
        self.visible_count = [2]

    def __sizeof__(self):
        return len(self.wchars)

    def __getitem__(self, pos):
        return self.wchars[pos]

    def findById(self, cid):
        return self.pos(WChar(cid, False, '', None, None))

    def subseq(self, start, end):
        seq = []
        for i in range(start+1, end):
            seq.append(i)
        return seq

    def contains(self, c):
        return self.pos(c) != -1

    def value(self):
        # print self.wchars
        s = reduce(lambda s, w:
                   s + (w.alpha if w.visible else ''),
                   self.wchars[1:len(self.wchars)-1], '')
        return s

    def pos(self, c):
        # print "pos search ", c.identifier
        # for i, w in enumerate(self.wchars):
        #     print "\t", w
        #     if w == c:
        #         return i
        # raise ValueError
        return self.wchars.index(c)

    @staticmethod
    def index_for(pos, array):
        begin = 0
        end = len(array)-1
        count = sum(array)
        while end - begin > 1:
            mid = (end + begin)/2
            mid_count = count - sum(array[mid:end+1])
            if pos < mid_count:
                end = mid-1
                count = mid_count
            elif pos == mid_count:
                end = mid
                count = mid_count
            else:
                begin = mid
        count = sum(array[:begin])
        if pos < count + array[begin]:
            assert(pos >= count)
            return begin, count  # uptil this segment
        else:
            count += array[begin] if begin != end else 0
            # can't check upper bound as this might be an append
            assert(pos >= count)
            return end, count  # uptil this segment
        assert(0)

    def putIndex(self, c, n_idx, pos):
        idx, count_uptil = WString.index_for(n_idx, self.count)
        if c != '\n':
            self.count[idx] += 1
            self.visible_count[idx] += 1
        else:
            visible_count_uptil = sum(self.visible_count[:idx])
            pos -= visible_count_uptil
            assert(pos >= 0)
            # this char is pos visible char in self.count[idx]
            w_char = 0
            p_char = -1
            for i in range(self.count[idx]):
                if self.wchars[count_uptil + i].visible:
                    p_char += 1
                    if p_char == pos:
                        break
                w_char += 1
            # assert because all insertions are between kBegin and kEnd.
            # so a seperator cannot be 'appended'
            assert(w_char < self.count[idx])
            self.count.insert(idx+1,
                              self.count[idx] - w_char)
            self.visible_count.insert(idx + 1,
                                      self.visible_count[idx] - p_char)
            self.count[idx] = w_char + 1
            self.visible_count[idx] = p_char + 1

    def ithVisibleLinear(self, i, start_offset=0):
        assert(i >= 0 and i < len(self.wchars))
        vis_count = -1
        idx = start_offset
        for w in self.wchars[start_offset:]:
            if w.visible:
                vis_count += 1
                if vis_count == i:
                    return idx
            idx += 1
        """
        return reduce(lambda vc, w: vc + (1 if w.visible
                                          and vc <= i else 0),
                      self.wchars[start_offset:], -1)  # return -1
        """

    def ithVisible(self, i):
        str_idx, visi_count_uptil = WString.index_for(i, self.visible_count)
        # there are the any wchars until this index
        count_uptil = sum(self.count[:str_idx])
        i -= visi_count_uptil
        assert(i >= 0)
        # now find ith visible char in self.wchars, starting
        # at offset count_uptil
        # count_uptil = 0
        return self.ithVisibleLinear(i, count_uptil)

    def integrateInsImpl(self, wc, p_idx, n_idx):
        seq = range(p_idx+1, n_idx)  # self.subseq(p_idx, n_idx)
        if len(seq) == 0:
            # sys.stderr.write("ins wc %s at %d\n" % (wc.alpha, n_idx))
            return n_idx  # n_idx was the char index of next char.
        p_pos = self.pos(self.wchars[p_idx])
        n_pos = self.pos(self.wchars[n_idx])

        def posCheck(idx):
            d = self.wchars[idx]
            cp_cond = self.findById(d.id_cp) <= p_pos
            cn_cond = n_pos <= self.findById(d.id_cn)
            return cp_cond and cn_cond

        L = filter(lambda i: posCheck(i), seq)
        L.insert(0, p_idx)
        L.append(n_idx)
        i = 1
        # sole place where id check is used
        while i < len(L) - 1 and self.wchars[L[i]] < wc:
            i += 1
        # sys.stderr.write("recurse ins %s < %s < %s\n" %
        #                 (self.wchars[p_idx].alpha,
        #                  wc.alpha,
        #                  self.wchars[n_idx].alpha))
        return self.integrateInsImpl(wc, L[i-1], L[i])

    def integrateIns(self, cid, alpha, pos):
        # we integrate *before* pos + 1, therefore
        # it works well with end char
        cp_idx = self.ithVisible(pos)
        cn_idx = self.ithVisible(pos+1)
        cp = self.wchars[cp_idx]
        cn = self.wchars[cn_idx]
        wc = WChar(cid, True, alpha, cp.identifier, cn.identifier)
        n_idx = self.integrateInsImpl(wc, cp_idx, cn_idx)
        self.wchars.insert(n_idx, wc)
        self.putIndex(wc.alpha, n_idx, pos+1)
        op = ','.join(str(x) for x in ['I',
                                       cp.identifier[0],
                                       cp.identifier[1],
                                       wc.identifier[0],
                                       wc.identifier[1],
                                       wc.alpha,
                                       cn.identifier[0],
                                       cn.identifier[1]])
        self.log("%s I %s, %s, %s, %s" % (hex(id(self)),
                                          cp.identifier,
                                          wc.identifier,
                                          repr(wc.alpha),
                                          cn.identifier))
        return op

    def integrateRemoteIns(self, wc):
        cp_idx = self.findById(wc.id_cp)
        cn_idx = self.findById(wc.id_cn)
        n_idx = self.integrateInsImpl(wc, cp_idx, cn_idx)
        self.wchars.insert(n_idx, wc)
        str_idx, count_uptil = WString.index_for(n_idx, self.count)
        nv = sum(self.visible_count[:str_idx])
        for i in range(self.count[str_idx]):
            if count_uptil + i >= n_idx:
                break
            if self.wchars[count_uptil + i].visible:
                nv += 1
        self.log("%s RI %s %s @ pos %d before %s (%s)" %
                 (hex(id(self)), str(wc.identifier),
                  repr(wc.alpha), nv - 1, str(self.wchars[n_idx+1].identifier),
                  repr(self.wchars[n_idx+1].alpha)))
        self.putIndex(wc.alpha, n_idx, nv)
        return wc.alpha, nv - 1

    def integrateRemoteDel(self, cid):
        idx = self.findById(cid)
        assert(self.wchars[idx] != kBegin and
               self.wchars[idx] != kEnd)
        if not self.wchars[idx].visible:
            """
            seed(16)
            I,2,3,2,6,\n,2,5
            RI (2, 6) '\n' 9 @ pos 3 before '\n'
            RI (1, 5) 'y' 6 @ pos 1 before 'c'
            I,2,5,1,6,y,32768,0
            D,2,3 ('c')
            RD (2, 3) 'c' @ pos 2, idx 8
            RI (1, 6) 'y' 11 @ pos 4 before '\n'
            D,2,6 ('\n')
            D,2,6 ('\n') <--- !!!
            """
            sys.stderr.write(
                "%s err exact char was already deleted?\n" % (hex(id(self))))
            return -1  # TBD
        # pos = reduce(lambda c, w: c + 1 if w.visible else 0,
        #             self.wchars[:idx+1], -1)
        pos = 0
        for i in range(idx):
            if self.wchars[i].visible:
                pos += 1
        str_idx, count_uptil = WString.index_for(idx, self.count)
        nv = sum(self.visible_count[:str_idx])
        for i in range(self.count[str_idx]):
            if count_uptil + i >= idx:
                break
            if self.wchars[count_uptil + i].visible:
                nv += 1
        if pos != nv:
            sys.stderr.write("BUG! \n")
        self.log("%s RD %s %s @ pos %d, idx %d" %
                 (hex(id(self)), str(self.wchars[idx].identifier),
                  repr(self.wchars[idx].alpha), pos - 1, idx))
        self.visible_count[str_idx] -= 1
        assert(self.visible_count[str_idx] >= 0)
        self.wchars[idx].visible = False
        # delete wc.alpha in the document *before*
        # the (visible) idx pos.
        # However, reduce pos by 1 as begin need not be returned
        return pos - 1

    def integrateDel(self, pos):
        # The index from the document is 0 indexed
        # Increment to account for kBegin
        pos += 1
        idx = self.ithVisible(pos)
        if self.wchars[idx] == kEnd:
            return ''
        # shouldn't idenitifer be sent for both ins and del
        # paper doesn't make it explicit
        identifier = self.wchars[idx].identifier
        op = ','.join(str(x) for x in ['D',
                                       identifier[0],
                                       identifier[1]])
        self.log("%s %s (%s) at pos %d" % (hex(id(self)), op,
                                           repr(self.wchars[idx].alpha),
                                           pos - 1))
        str_idx, _ = WString.index_for(idx, self.count)
        self.visible_count[str_idx] -= 1
        assert(self.visible_count[str_idx] >= 0)
        self.wchars[idx].visible = False
        return op

    def cmp(self, c, d):
        return self.pos(c) <= self.pos(d)

    def log(self, msg):
        # counts = ', '.join(str(n)+'/'+str(d) for n, d in zip(self.count,
        #                                                     self.visible_count))
        sys.stderr.write("%s \n" % (msg))


class WootNote:
    def __init__(self, site, clock):
        self.wstring = WString()
        self.site = site
        self.clock = clock
        self.history = []

    def generateIns(self, pos, alpha):
        self.clock += 1
        ins_str = self.wstring.integrateIns((self.site, self.clock),
                                            alpha, pos)
        self.history.append(ins_str)
        return ins_str

    def generateDel(self, pos):
        del_str = self.wstring.integrateDel(pos)
        self.history.append(del_str)
        return del_str

    def remoteIns(self, ins_str):
        self.history.append(ins_str)
        # I,cp_site,cp_clock,c_site,c_clock,alpha,cn_site,cn_clock\n
        ins_op = ins_str.rstrip('\n').split(',')
        id_cp = (int(ins_op[1]), int(ins_op[2]))
        id_cn = (int(ins_op[-2]), int(ins_op[-1]))
        wc_id = (int(ins_op[3]), int(ins_op[4]))
        wch = WChar(wc_id, True, ins_op[5], id_cp, id_cn)
        return self.wstring.integrateRemoteIns(wch)

    def remoteDel(self, del_str):
        self.history.append(del_str)
        # D,cp_site,cp_clock\n
        del_op = del_str.rstrip('\n').split(',')
        return self.wstring.integrateRemoteDel((int(del_op[1]),
                                                int(del_op[2])))

    def value(self):
        return self.wstring.value()

    def replay(self):
        for h in self.history:
            sys.stderr.write("%s" % (h))


class WootThread(threading.Thread):
    def __init__(self, site):
        threading.Thread.__init__(self)
        self.wootNote = WootNote(site, 0)
        self.queue = deque()
        self.otherQueue = None

    def run(self):
        # chars = 'abcdefghijklmnopqrstuvwxyz\n'
        chars = 'abcxyz\n\n\n\n'
        done = False
        exitPending = False
        visible_count = 0
        while not done:
            time.sleep(0.1)
            while len(self.queue) != 0:
                op = self.queue.popleft()
                if op[0] == 'I':
                    self.wootNote.remoteIns(op)
                    visible_count += 1
                elif op[0] == 'D':
                    self.wootNote.remoteDel(op)
                    visible_count -= 1
                elif op[0] == 'q':
                    exitPending = True
                elif op[0] == 'Q':
                    done = True
            if exitPending:
                continue  # when exit is pending, don't generate
            # simulate ins or del randomly if vis_count > 0
            # vis count is incremented after insert.
            if visible_count == 0:
                op = 0
                pos = 0
            else:
                op = random.randint(0, 1)
                pos = random.randint(0, visible_count)
            if op == 0:
                alpha = chars[random.randint(0, len(chars)-1)]
                op_str = self.wootNote.generateIns(pos, alpha)
            else:
                op_str = self.wootNote.generateDel(pos)
            if op_str != '':
                visible_count = visible_count + (1 if op == 0 else -1)
                self.otherQueue.append(op_str)


def concurrent_test(seed=17):
    random.seed(seed)
    wt1 = WootThread(1)
    wt2 = WootThread(2)
    wt1.otherQueue = wt2.queue
    wt2.otherQueue = wt1.queue
    wt1.start()
    wt2.start()
    time.sleep(2)
    wt1.queue.append('q')
    wt2.queue.append('q')
    time.sleep(1)
    wt1.queue.append('Q')
    wt2.queue.append('Q')
    wt1.join()
    wt2.join()
    return repr(wt1.wootNote.value()) == repr(wt2.wootNote.value())


def prettyprint(wn):
    for w in wn.wstring.wchars:
        if w.visible:
            sys.stderr.write("%s" % (repr(w.alpha)))
        else:
            sys.stderr.write("$")
    sys.stderr.write("\n")


def printithvisible(wn, i):
    print repr(wn.wstring.wchars[wn.wstring.ithVisible(i)].alpha)
