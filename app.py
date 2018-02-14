from chavosh import reader
from chavosh import filters
import pickle
from sys import argv
import logging
from dateutil import parser

logging.basicConfig(format='%(asctime)-15s %(message)s')
logger = logging.getLogger("read-chavosh")
logger.setLevel(logging.INFO)


def print_stats(stat_filter):
    print('--- Stats ---')
    print(str(stat_filter))
    print()


def print_similar_stats(similar_filter):
    print('--- SimilarFilter ---')
    print(str(similar_filter))
    print()


def convert(input, output):
    logger.info('opening input file %s' % input)
    with open(input) as f:
        logging.info('opening output file %s' % output)
        with open(output, 'wb') as of:
            logger.info('reading data from file')
            data = filters.to_list(reader.read(f), True)
            logger.info('reading data done, writing dump to output file')
            pickle.dump(data, of, pickle.HIGHEST_PROTOCOL)
            logger.info('done!')


def stats(input):
    logger.info('opening input file %s' % input)
    with open(input, 'rb') as f:
        logger.info('reading data from file')
        data = pickle.load(f)
        logger.info('done reading file')

        stat_filter = filters.StatFilter()
        seq = stat_filter.collect(data)

        similar_filter = filters.StatSimilarFilter()
        seq = similar_filter.collect(seq)

        filters.to_list(seq)

        print_stats(stat_filter)
        print_similar_stats(similar_filter)


def stats_raw(input):
    logger.info('opening input file %s' % input)
    with open(input) as f:
        logger.info('file opened')
        data = reader.read(f)

        stat_filter = filters.StatFilter()
        seq = stat_filter.collect(data)

        similar_filter = filters.StatSimilarFilter()
        seq = similar_filter.collect(seq)

        def callback():
            print_stats(stat_filter)
            print_similar_stats(similar_filter)

        filters.enumerate_ticked(seq, callback)
        print()
        print()
        print()

        callback()


def filter_similar_filter_reviewer_rejected_accept_raw(input):
    logger.info('opening input file %s' % input)
    with open(input) as f:
        logger.info('file opened')
        data = reader.read(f)

        similar_filter = filters.StatSimilarFilter()
        seq = similar_filter.filter_reviewer_rejected_accept(data)

        for log in seq:
            print(str(log))

        print()
        print()
        print()


def filter_similar_filter_reviewer_rejected_accept(input, output):
    logger.info('opening input file %s' % input)
    with open(input) as f:
        logger.info('opening output file %s' % output)
        with open(output, 'wb') as out:
            logger.info('file opened')
            data = reader.read(f)

            similar_filter = filters.StatSimilarFilter()
            seq = similar_filter.filter_reviewer_rejected_accept(data)

            def callback(collected_list):
                logger.info('collected %d items so far' % len(collected_list))

            out_list = filters.to_list_ticked(seq, callback)
            callback(out_list)

            pickle.dump(out_list, out, pickle.HIGHEST_PROTOCOL)
            logger.info('done!')


def list_file(input, tokens=False):
    logger.info('opening input file %s' % input)
    with open(input, 'rb') as f:
        logger.info('reading data from file')
        data = pickle.load(f)
        logger.info('done reading file')

        if tokens:
            for log in data:
                print(log.token)
        else:
            for log in data:
                print(str(log))

        print()


def filter_date_pipe(input, from_date, to_date):
    logger.info('opening input file %s' % input)
    with open(input) as f:
        logger.info('file opened')
        data = reader.read(f)

        for log in data:
            if log.time >= from_date and (not to_date or log.time < to_date):
                for line in log.lines:
                    print(line)


if argv[1] == 'convert':
    convert(argv[2], argv[3])
elif argv[1] == 'stats':
    stats(argv[2])
elif argv[1] == 'stats-raw':
    stats_raw(argv[2])
elif argv[1] == 'filter-similarfilter-reviewer-rejected-accept-raw':
    filter_similar_filter_reviewer_rejected_accept_raw(argv[2])
elif argv[1] == 'filter-similarfilter-reviewer-rejected-accept':
    filter_similar_filter_reviewer_rejected_accept(argv[2], argv[3])
elif argv[1] == 'list':
    list_file(argv[2], '--token' in argv[3:])
elif argv[1] == 'filter-date-pipe':
    filter_date_pipe(argv[2], parser.parse(argv[3]), parser.parse(argv[4]) if len(argv) >= 5 and argv[4] else None)
