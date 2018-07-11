import sys


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
            self.count.insert(idx, w_char + 1)
            self.visible_count.insert(idx, p_char + 1)

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
        sys.stderr.write("%s \n" % (op))
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
        sys.stderr.write("RI %s %s %d @ pos %d before %s\n" %
                         (str(wc.identifier),
                          repr(wc.alpha), n_idx,
                          nv - 1, repr(self.wchars[n_idx+1].alpha)))
        self.putIndex(wc.alpha, n_idx, nv)
        return wc.alpha, nv - 1

    def integrateRemoteDel(self, cid):
        idx = self.findById(cid)
        assert(self.wchars[idx] != kBegin and
               self.wchars[idx] != kEnd)
        assert(self.wchars[idx].visible)
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
        assert(pos == nv)
        self.visible_count[str_idx] -= 1
        assert(self.visible_count[str_idx] >= 0)
        self.wchars[idx].visible = False
        sys.stderr.write("RD %s %s @ pos %d, idx %d\n" %
                         (str(self.wchars[idx].identifier),
                          repr(self.wchars[idx].alpha), pos - 1, idx))
        # delete wc.alpha in the document *before*
        # the (visible) idx pos.
        # However, reduce pos by 1 as begin need not be returned
        return pos - 1

    def integrateDel(self, pos):
        # The index from the document is 0 indexed
        # Increment to account for kBegin
        pos += 1
        idx = self.ithVisible(pos)
        str_idx, _ = WString.index_for(idx, self.count)
        self.visible_count[str_idx] -= 1
        assert(self.visible_count[str_idx] >= 0)
        self.wchars[idx].visible = False
        # shouldn't idenitifer be sent for both ins and del
        # paper doesn't make it explicit
        identifier = self.wchars[idx].identifier
        op = ','.join(str(x) for x in ['D',
                                       identifier[0],
                                       identifier[1]])
        sys.stderr.write("%s (%s)\n" % (op, repr(self.wchars[idx].alpha)))
        return op

    def cmp(self, c, d):
        return self.pos(c) <= self.pos(d)


class WootNote:
    def __init__(self, site, clock):
        self.wstring = WString()
        self.site = site
        self.clock = clock

    def generateIns(self, pos, alpha):
        self.clock += 1
        return self.wstring.integrateIns((self.site, self.clock),
                                         alpha, pos)

    def generateDel(self, pos):
        return self.wstring.integrateDel(pos)

    def remoteIns(self, ins_str):
        # I,cp_site,cp_clock,c_site,c_clock,alpha,cn_site,cn_clock\n
        ins_op = ins_str.rstrip('\n').split(',')
        id_cp = (int(ins_op[1]), int(ins_op[2]))
        id_cn = (int(ins_op[-2]), int(ins_op[-1]))
        wc_id = (int(ins_op[3]), int(ins_op[4]))
        wch = WChar(wc_id, True, ins_op[5], id_cp, id_cn)
        return self.wstring.integrateRemoteIns(wch)

    def remoteDel(self, del_str):
        # D,cp_site,cp_clock\n
        del_op = del_str.rstrip('\n').split(',')
        return self.wstring.integrateRemoteDel((int(del_op[1]),
                                                int(del_op[2])))

    def value(self):
        return self.wstring.value()


def prettyprint(wn):
    for w in wn.wstring.wchars:
        if w.visible:
            sys.stderr.write("%s" % (repr(w.alpha)))
        else:
            sys.stderr.write("$")
    sys.stderr.write("\n")


def printithvisible(wn, i):
    print repr(wn.wstring.wchars[wn.wstring.ithVisible(i)].alpha)
