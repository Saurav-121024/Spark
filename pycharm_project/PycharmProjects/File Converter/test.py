import multiprocessing, sys, time, os, mmap
import logging, logging.handlers


def init_logger(pid):
    console_format = 'P{0} %(levelname)s %(message)s'.format(pid)
    logger = logging.getLogger()  # New logger at root level
    logger.setLevel(logging.INFO)
    logger.handlers.append(logging.StreamHandler())
    logger.handlers[0].setFormatter(logging.Formatter(console_format, '%d/%m/%y %H:%M:%S'))


def getFileLineCount(queues, pid, processes, file1):
    init_logger(pid)
    logging.info('start')

    physical_file = open(file1, "r")
    #  mmap.mmap(fileno, length[, tagname[, access[, offset]]]

    m1 = mmap.mmap(physical_file.fileno(), 0, access=mmap.ACCESS_READ)

    # work out file size to divide up line counting

    fSize = os.stat(file1).st_size
    chunk = (fSize / processes) + 1

    lines = 0

    # get where I start and stop
    _seedStart = chunk * (pid)
    _seekEnd = chunk * (pid + 1)
    seekStart = int(_seedStart)
    seekEnd = int(_seekEnd)

    if seekEnd < int(_seekEnd + 1):
        seekEnd += 1

    if _seedStart < int(seekStart + 1):
        seekStart += 1

    if seekEnd > fSize:
        seekEnd = fSize

    # find where to start
    if pid > 0:
        m1.seek(seekStart)
        # read next line
        l1 = m1.readline()  # need to use readline with memory mapped files
        seekStart = m1.tell()

    # tell previous rank my seek start to make their seek end

    if pid > 0:
        queues[pid - 1].put(seekStart)
    if pid < processes - 1:
        seekEnd = queues[pid].get()

    m1.seek(seekStart)
    l1 = m1.readline()

    while len(l1) > 0:
        lines += 1
        l1 = m1.readline()
        if m1.tell() > seekEnd or len(l1) == 0:
            break

    logging.info('done')
    # add up the results
    if pid == 0:
        for p in range(1, processes):
            lines += queues[0].get()
        queues[0].put(lines)  # the total lines counted
    else:
        queues[0].put(lines)

    m1.close()
    physical_file.close()


if __name__ == '__main__':
    init_logger('main')
    if len(sys.argv) > 1:
        file_name = sys.argv[1]
    else:
        logging.fatal('parameters required: file-name [processes]')
        exit()

    t = time.time()
    processes = multiprocessing.cpu_count()
    if len(sys.argv) > 2:
        processes = int(sys.argv[2])
    queues = []  # a queue for each process
    for pid in range(processes):
        queues.append(multiprocessing.Queue())
    jobs = []
    prev_pipe = 0
    for pid in range(processes):
        p = multiprocessing.Process(target=getFileLineCount, args=(queues, pid, processes, file_name,))
        p.start()
        jobs.append(p)

    jobs[0].join()  # wait for counting to finish
    lines = queues[0].get()

    logging.info('finished {} Lines:{}'.format(time.time() - t, lines))
