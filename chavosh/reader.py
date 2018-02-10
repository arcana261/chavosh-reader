from dateutil import parser
import ast
import chavosh.models
from chavosh.log_entry import Log
import logging

logging.basicConfig(format='%(asctime)-15s %(message)s')
logger = logging.getLogger("read-chavosh")
logger.setLevel(logging.INFO)

def _split(str):
    return [x.strip() for x in str.split(':', 1)]

def read(seq):
    step = 0
    t = None
    token = None
    result = None
    reviewer_reject_reason_id = None
    similar_post_tokens = []
    is_system = False
    is_bot = False
    lines = []

    line_number = 0

    for line in (x.strip() for x in seq):
        line_number = line_number + 1
        if line == '':
            continue

        try:
            h, v = _split(line)
        except ValueError:
            logger.error('could not unpack by : in line (%d) with value %s' % (line_number, line))
            continue

        if step == 0:
            if h == 'Time':
                t = parser.parse(v)
                lines.append(line)
                step = 1
            else:
                logger.error('expected Time: in line %d but got %s' % (line_number, line))
        elif step == 1:
            if h == 'Token':
                token = v
                lines.append(line)
                step = 2
            else:
                t = token = result = reviewer_reject_reason_id = similar_post_tokens = None
                is_system = is_bot = False
                lines = []
                step = 0
                logger.error('expected Token: in line %d but got %s' % (line_number, line))
        elif step == 2:
            if h == 'IsSystem':
                try:
                    is_system = ast.literal_eval(v)
                    lines.append(line)
                except:
                    t = token = result = reviewer_reject_reason_id = similar_post_tokens = None
                    is_system = is_bot = False
                    lines = []
                    step = 0

                    logger.error('failed to parse IsSystem: value %s in line %d with content %s' % (v, line_number, line))

                    index = v.rfind('Time:')
                    if index >= 0:
                        v = v[index:]
                        t = parser.parse(_split(v)[1])
                        lines.append(v)
                        step = 1

            elif h == 'Result':
                try:
                    d = ast.literal_eval(v)
                except:
                    t = token = result = reviewer_reject_reason_id = similar_post_tokens = None
                    is_system = is_bot = False
                    lines = []
                    step = 0

                    logger.error('failed to parse Result: value %s in line %d with content %s' % (v, line_number, line))

                    index = v.rfind('Time:')
                    if index >= 0:
                        v = v[index:]
                        t = parser.parse(_split(v)[1])
                        lines.append(v)
                        step = 1

                else:
                    result = chavosh.models.Result()

                    if d:
                        if 'decision' in d:
                            decision = d['decision']
                            val = chavosh.models.Decision()
                            if 'judge' in decision:
                                val.judge = decision['judge'].lower()
                            if 'ignore' in decision:
                                val.ignore = decision['ignore']
                            if 'debug' in decision:
                                val.debug = decision['debug']
                            if 'similar_posts' in decision:
                                val.similar_posts = decision['similar_posts']
                            if 'filter' in decision:
                                val.filter = decision['filter']
                            if 'review_reason' in decision:
                                val.review_reason = decision['review_reason']
                            if 'reject_reason_id' in decision:
                                val.reject_reason_id = decision['reject_reason_id']
                            result.decision = val

                        if 'filters' in d:
                            filters = d['filters']
                            val = chavosh.models.Filters()

                            if 'SimilarFilter' in filters:
                                similar_filter = filters['SimilarFilter']
                                x = chavosh.models.SimilarFilter()

                                if 'reject_reason_id' in similar_filter:
                                    x.reject_reason_id = similar_filter['reject_reason_id']
                                if 'ignore' in similar_filter:
                                    x.ignore = similar_filter['ignore']
                                if 'judge' in similar_filter:
                                    x.judge = similar_filter['judge'].lower()
                                if 'similar_posts' in similar_filter:
                                    x.similar_posts = similar_filter['similar_posts']
                                if 'review_reason' in similar_filter:
                                    x.review_reason = similar_filter['review_reason']
                                if 'debug' in similar_filter:
                                    x.debug = similar_filter['debug']

                                val.similar_filter = x

                            result.filters = val

                    lines.append(line)
                    step = 3
            else:
                t = token = result = reviewer_reject_reason_id = similar_post_tokens = None
                is_system = is_bot = False
                lines = []
                step = 0

                logger.error('expected IsSystem: or Result: in line %d but got %s' % (line_number, line))
        elif step == 3:
            if h == 'IsBot':
                try:
                    is_bot = ast.literal_eval(v)
                    lines.append(line)
                except:
                    t = token = result = reviewer_reject_reason_id = similar_post_tokens = None
                    is_system = is_bot = False
                    lines = []
                    step = 0

                    logger.error('failed to parse IsBot: value %s in line %d with content %s' % (v, line_number, line))

                    index = v.rfind('Time:')
                    if index >= 0:
                        v = v[index:]
                        t = parser.parse(_split(v)[1])
                        lines.append(v)
                        step = 1

            elif h == 'ReviewerRejectReasonId':
                if len(v) > 0:
                    reviewer_reject_reason_id = int(v)
                else:
                    reviewer_reject_reason_id = 0
                lines.append(line)
                step = 4
            else:
                t = token = result = reviewer_reject_reason_id = similar_post_tokens = None
                is_system = is_bot = False
                lines = []
                step = 0

                logger.error('expected IsBot: or ReviewerRejectReasonId: in line %d but got %s' % (line_number, line))
        elif step == 4:
            if h == 'SimilarPostTokens':
                if len(v) > 0:
                    v = v.replace('<OngoingPost: ', "'").replace('>', "'")
                    try:
                        similar_post_tokens = ast.literal_eval(v)
                    except:
                        t = token = result = reviewer_reject_reason_id = similar_post_tokens = None
                        is_system = is_bot = False
                        lines = []
                        step = 0

                        logger.error('failed to parse SimilarPostTokens: value %s in line %d with content %s' % (v, line_number, line))

                        index = v.rfind('Time:')
                        if index >= 0:
                            v = v[index:]
                            t = parser.parse(_split(v)[1])
                            lines.append(v)
                            step = 1
                else:
                    similar_post_tokens = []

                if t is not None and token is not None and result is not None and reviewer_reject_reason_id is not None and similar_post_tokens is not None:
                    log = Log()
                    log.time = t
                    log.token = token
                    log.result = result
                    log.reviewer_reject_reason_id = reviewer_reject_reason_id
                    log.similar_post_tokens = similar_post_tokens
                    log.is_system = is_system
                    log.is_bot = is_bot
                    log.is_reviewer = (not is_bot) and (not is_system)
                    lines.append(line)
                    log.lines = lines
                    yield log
                else:
                    incompletes = []
                    if t is None:
                        incompletes.append('time')
                    if token is None:
                        incompletes.append('token')
                    if result is None:
                        incompletes.append('result')
                    if reviewer_reject_reason_id is None:
                        incompletes.append('reviewer_reject_reason_id')
                    if similar_post_tokens is None:
                        incompletes.append('similar_post_tokens')

                    logger.error('incomplete log in line %d : %s' % (line_number, str(incompletes)))
            else:
                logger.error('expected SimilarPostTokens: in line %d but got %s' % (line_number, line))

            t = token = result = reviewer_reject_reason_id = similar_post_tokens = None
            is_system = is_bot = False
            lines = []
            step = 0


