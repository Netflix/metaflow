import platform
import select

class ProcPollEvent(object):
    def __init__(self, fd, can_read=False, is_terminated=False):
        self.fd = fd
        self.can_read = can_read
        self.is_terminated = is_terminated

class ProcPoll(object):

    def poll(self):
        raise NotImplementedError()

    def add(self, fd):
        raise NotImplementedError()

    def remove(self, fd):
        raise NotImplementedError()

class LinuxProcPoll(ProcPoll):

    def __init__(self):
        self._poll = select.poll()

    def add(self, fd):
        self._poll.register(fd, select.POLLIN |
                                select.POLLERR |
                                select.POLLHUP)

    def remove(self, fd):
        self._poll.unregister(fd)

    def poll(self, timeout):
        for (fd, event) in self._poll.poll(timeout):
            yield ProcPollEvent(fd=fd,
                                can_read=bool(event & select.POLLIN),
                                is_terminated=bool(event & select.POLLHUP) or
                                              bool(event & select.POLLERR))


class DarwinProcPoll(ProcPoll):

    def __init__(self):
        self._kq = select.kqueue()

    def add(self, fd):
        ev = select.kevent(fd,
                           filter=select.KQ_FILTER_READ,
                           flags=select.KQ_EV_ADD)
        self._kq.control([ev], 0, 0)

    def remove(self, fd):
        ev = select.kevent(fd,
                           flags=select.KQ_EV_DELETE)
        self._kq.control([ev], 0, 0)

    def poll(self, timeout):
        for event in self._kq.control(None, 100, timeout):
            yield ProcPollEvent(fd=event.ident,
                                can_read=True,
                                is_terminated=event.flags & select.KQ_EV_EOF)

def make_poll():
    os = platform.system()
    if os == 'Linux':
        return LinuxProcPoll()
    elif os == 'Darwin':
        return DarwinProcPoll()
    else:
        raise Exception("Polling is not supported on "
                        "your operating system (%s)" % os)

if __name__ == '__main__':
    import subprocess
    p1 = subprocess.Popen(['bash', '-c',
                           'for ((i=0;i<10;i++)); '
                           'do echo "first $i"; sleep 1; done'],
                          bufsize=1,
                          stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE)
    p2 = subprocess.Popen(['bash', '-c',
                           'for ((i=0;i<5;i++)); '
                           'do echo "second $i"; sleep 2; done'],
                          bufsize=1,
                          stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE)

    fds = {p1.stdout.fileno(): ('p1', p1.stdout),
           p2.stdout.fileno(): ('p2', p2.stdout)}

    poll = make_poll()
    print('poller is %s' % poll)

    for fd in fds:
        poll.add(fd)

    n = 2
    while n > 0:
        for event in poll.poll(0.5):
            name, fileobj = fds[event.fd]
            print('[%s] %s' % (name, fileobj.readline().strip()))
            if event.is_terminated:
                print('[%s] terminated' % name)
                poll.remove(event.fd)
                n -= 1
