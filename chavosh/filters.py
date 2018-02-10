from logging import getLogger
from logging import INFO
from logging import basicConfig
from time import clock
import string
import random
import pprint
import redis
import pickle
import base64

basicConfig(format='%(asctime)-15s %(message)s')
logger = getLogger(__name__)
logger.setLevel(INFO)

connection = redis.StrictRedis(host='localhost', port=6379, db=0)


def enumerate_ticked(seq, cb, every=3):
    t = clock() if every > 0 else 0
    for x in seq:
        t2 = clock()
        if every > 0:
            if t2 - t >= every:
                t = t2
                cb()


def to_list_ticked(seq, cb, every=3):
    result = []
    t = clock() if every > 0 else 0
    for x in seq:
        result.append(x)
        t2 = clock()
        if every > 0:
            if t2 - t >= every:
                t = t2
                cb(result)

    return result


def to_list(seq, log=False):
    result = to_list_ticked(seq, lambda x: logger.info('processed %d so far...' % len(x)), 3 if log else 0)
    if log:
        logger.info('finished!')
    return result


def _pretify(indent, v):
    if isinstance(v, set) or isinstance(v, BigSet):
        return _pretify_set(indent, v)
    elif isinstance(v, dict) or isinstance(v, BigDictionary):
        return _pretify_dict(indent, v)
    elif hasattr(v, '__pretify__'):
        return v.__pretify__(indent)
    else:
        return indent + str(v)


def _pretify_keyvalue(indent, k, v):
    return _pretify(indent, k) + ':' + '\n' + _pretify(indent + ' ', v)


def _pretify_set(indent, s):
    result = indent
    first = True
    for x in s:
        if first:
            first = False
        else:
            result = result + ', '
        result = result + _pretify('', x)
    return result


def _pretify_dict(indent, s):
    if '_compact_pretify__' in s:
        result = indent
        first = True
        for k, v in (s.iteritems if hasattr(s, 'iteritems') else s.items)():
            if k != '_compact_pretify__':
                if first:
                    first = False
                else:
                    result = result + ', '
                result = result + _pretify('', k) + ': ' + _pretify('', v)
    else:
        result = ''
        first = True
        for k, v in (s.iteritems if hasattr(s, 'iteritems') else s.items)():
            if k != '_compact_pretify__':
                if first:
                    first = False
                else:
                    result = result + '\n'
                result = result + _pretify_keyvalue(indent, k, v)
    return result


def _rand_key():
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(11))


def _serialize(value):
    return base64.b64encode(pickle.dumps(value)).decode('ascii')


def _deserialize(value):
    return pickle.loads(base64.b64decode(value))


class BigSet:
    def __init__(self):
        self.key = 'read_chavosh:bigset:%s' % _rand_key()
        connection.delete(self.key)

    def add(self, item):
        connection.sadd(self.key, _serialize(item))

    def remove(self, item):
        connection.srem(self.key, _serialize(item))

    def __len__(self):
        return connection.scard(self.key)

    def __iter__(self):
        for item in connection.sscan_iter(self.key):
            yield _deserialize(item)

    def __contains__(self, item):
        return connection.sismember(self.key, _serialize(item))


class BigSortedSet:
    def __init__(self):
        self.key = 'read_chavosh:bigsset:%s' % _rand_key()

    def add(self, item):
        connection.zadd(self.key, item, _serialize(item))

    def remove(self, item):
        connection.zrem(self.key, _serialize(item))

    def __len__(self):
        return connection.zcard(self.key)

    def __contains__(self, item):
        return connection.zrank(self.key, _serialize(item)) is not None

    def __iter__(self):
        for item in connection.zscan_iter(self.key):
            yield _deserialize(item)


class BaseBigDictionary:
    def __init__(self, items):
        self.key = 'read_chavosh:bigdict:value:%s:%%s' % _rand_key()
        self.items = items

    def __setitem__(self, key, value):
        connection.set(self.key % key, _serialize(value))
        self.items.add(key)

    def __getitem__(self, key):
        if key in self.items:
            value = connection.get(self.key % key)
            if not value:
                raise KeyError()
            return _deserialize(value)
        else:
            raise KeyError()

    def get(self, key, default):
        if key in self.items:
            value = connection.get(self.key % key)
            if not value:
                raise KeyError()
            return _deserialize(value)
        else:
            return default

    def __contains__(self, item):
        return item in self.items

    def iteritems(self):
        for key in self.items:
            yield (key, self.get(key, None))

    def items(self):
        return [x for x in self.iteritems()]


class BigDictionary(BaseBigDictionary):
    def __init__(self):
        super(BigDictionary, self).__init__(BigSet())


class BigSortedDictionary(BaseBigDictionary):
    def __init__(self):
        super(BigSortedDictionary, self).__init__(BigSortedSet())


class ReasonStore:
    __reasons = {
        'picture': {46},
        'word_change': {7, 9, 13, 16, 20, 23, 24, 25, 32, 33, 37, 38, 45},
        '_duplicate': {1, 80, 3, 40, 18},
        '_incorrect_title': {10, 16, 23, 26, 32, 33, 7, 25, 37, 63},
        '_incorrect_data': {31, 13, 47, 28, 49, 20, 11, 15, 51, 65, 24, 29, 35, 46, 48, 53, 55, 56},
        '_incorrect_category': {19, 4, 21, 43, 5, 52, 54, 27, 36},
        '_denied_post': {12, 30, 41, 17, 34, 57, 64, 67, 9, 2, 6, 14, 22, 38, 44, 50, 68, 69, 600, 102, 104},
    }

    def __init__(self):
        self.reasons = dict()
        self.reasons['_compact_pretify__'] = True

        self.target_reasons = dict()

        for title in ReasonStore.__reasons:
            self.target_reasons[title] = 0

        self.other = 0
        self.other_reasons = set()

    def add(self, reason):
        if reason > 0:
            if reason not in self.reasons:
                self.reasons[reason] = 1
            else:
                self.reasons[reason] = self.reasons[reason] + 1

            found = False

            for title, reason_set in ReasonStore.__reasons.items():
                if reason in reason_set:
                    self.target_reasons[title] = self.target_reasons[title] + 1
                    found = True

            if not found:
                self.other = self.other + 1
                self.other_reasons.add(reason)

    def __pretify__(self, indent):
        return _pretify(indent, {
            **self.target_reasons,
            'other': self.other,
            'other-reasons': self.other_reasons,
            'detail': self.reasons
        })


class StatItem:
    IGNORE = 'ignore'
    DECIDING = 'deciding'
    NEUTRALIZED = 'neutralized'
    MASKED = 'masked'

    def __init__(self):
        self.count = 0
        self.ignore = 0
        self.deciding = 0
        self.neutralized = 0
        self.masked = 0

    def add(self, item_type):
        self.count = self.count + 1
        if item_type == StatItem.IGNORE:
            self.ignore = self.ignore + 1
        elif item_type == StatItem.DECIDING:
            self.deciding = self.deciding + 1
        elif item_type == StatItem.NEUTRALIZED:
            self.neutralized = self.neutralized + 1
        elif item_type == StatItem.MASKED:
            self.masked = self.masked + 1
        else:
            raise ValueError("unknown type", item_type)

    def __pretify__(self, indent):
        return _pretify(indent, {
            'total': self.count,
            'ignored': self.ignore,
            'neutralized': self.neutralized,
            'masked': self.masked,
            'deciding': self.deciding,
        })


class CollectingTokenStatItem(StatItem):
    def __init__(self, collect_type):
        super(CollectingTokenStatItem, self).__init__()
        self.collect_type = collect_type
        self.tokens = BigSet()

    def add_token(self, item_type, token):
        super(CollectingTokenStatItem, self).add(item_type)
        if item_type == self.collect_type:
            self.tokens.add(token)


class CollectingReasonStatItem(StatItem):
    def __init__(self):
        super(CollectingReasonStatItem, self).__init__()
        self.reasons = ReasonStore()
        self.ignored_reasons = ReasonStore()
        self.deciding_reasons = ReasonStore()
        self.neutralized_reasons = ReasonStore()
        self.masked_reasons = ReasonStore()

    def add_reason(self, item_type, reason):
        super(CollectingReasonStatItem, self).add(item_type)
        if reason > 0:
            self.reasons.add(reason)
            if item_type == StatItem.IGNORE:
                self.ignored_reasons.add(reason)
            elif item_type == StatItem.DECIDING:
                self.deciding_reasons.add(reason)
            elif item_type == StatItem.NEUTRALIZED:
                self.neutralized_reasons.add(reason)
            elif item_type == StatItem.MASKED:
                self.masked_reasons.add(reason)
            else:
                raise ValueError("unknown type", item_type)

    def __pretify__(self, indent):
        return _pretify(indent, {
            'stats': super(CollectingReasonStatItem, self).__pretify__(indent + ' '),
            'all-reasons': self.reasons,
            'ignored-reasons': self.ignored_reasons,
            'neutralized-reasons': self.neutralized_reasons,
            'masked-reasons': self.masked_reasons,
            'deciding-reasons': self.deciding_reasons,
        })


class CollectingTokenReasonStatItem(CollectingReasonStatItem):
    def __init__(self, collect_type):
        super(CollectingTokenReasonStatItem, self).__init__()
        self.collect_type = collect_type
        self.tokens = BigSet()

    def add_token_reason(self, item_type, token, reason):
        super(CollectingTokenReasonStatItem, self).add_reason(item_type, reason)
        if item_type == self.collect_type:
            self.tokens.add(token)


class BaseCube:
    def __init__(self, dictionary):
        self.items = dictionary

    def get(self, key):
        return self.items.get(key, 0)

    def set(self, key, value):
        self.items[key] = value

    def add(self, key, value):
        self.items[key] = self.get(key) + value

    def increment(self, key):
        self.add(key, 1)

    def decrement(self, key):
        self.add(key, -1)

    def __pretify__(self, indent):
        return _pretify(indent, self.items)

    def __contains__(self, item):
        return item in self.items

    def __iter__(self):
        return self.items.__iter__()

    def items(self):
        return self.items.items()


class BigCube(BaseCube):
    def __init__(self):
        super(BigCube, self).__init__(BigDictionary())


class MemoryCube(BaseCube):
    def __init__(self):
        super(MemoryCube, self).__init__(dict())


class LeveledCollectingTokenStatItem(CollectingTokenStatItem):
    def __init__(self):
        super(LeveledCollectingTokenStatItem, self).__init__(StatItem.DECIDING)
        self._cube = BigCube()
        self.levels = MemoryCube()

    def add_related_token(self, item_type, token, related_token):
        super(LeveledCollectingTokenStatItem, self).add_token(item_type, token)
        if item_type == StatItem.DECIDING:
            new_level = self._cube.get(related_token) + 1
            if token in self._cube:
                prev_level = self._cube.get(token)
                self.levels.decrement(prev_level)
            self._cube.set(token, new_level)
            self.levels.increment(new_level)

    def __pretify__(self, indent):
        return _pretify(indent, {
            'stats': super(LeveledCollectingTokenStatItem, self).__pretify__(indent + ' '),
            'levels': self.levels,
        })


class LeveledCollectingTokenReasonStatItem(CollectingTokenReasonStatItem):
    def __init__(self):
        super(LeveledCollectingTokenReasonStatItem, self).__init__(StatItem.DECIDING)
        self._cube = BigCube()
        self.levels = MemoryCube()

    def add_related_token_reason(self, item_type, token, reject_reason, related_token):
        super(LeveledCollectingTokenReasonStatItem, self).add_token_reason(item_type, token, reject_reason)
        if item_type == StatItem.DECIDING:
            new_level = self._cube.get(related_token) + 1
            if token in self._cube:
                prev_level = self._cube.get(token)
                self.levels.decrement(prev_level)
            self._cube.set(token, new_level)
            self.levels.increment(new_level)

    def __pretify__(self, indent):
        return _pretify(indent, {
            'stats': super(LeveledCollectingTokenReasonStatItem, self).__pretify__(indent + ' '),
            'levels': self.levels,
        })


class AbstractStatFilter:
    class StatFilterData:
        def __init__(self, item_type, judge, reject_reason, deciding, neutralized, related_token, **kwargs):
            self.item_type = item_type
            self.judge = judge
            self.reject_reason = reject_reason
            self.deciding = deciding
            self.neutralized = neutralized
            self.related_token = related_token
            self.args = kwargs

    def __init__(self, total, accept, reject, duplicate, neutral, none):
        self.all = total
        self.accept = accept
        self.reject = reject
        self.duplicate = duplicate
        self.neutral = neutral
        self.none = none

    @staticmethod
    def _add_to(collector, log, data):
        if isinstance(collector, LeveledCollectingTokenReasonStatItem):
            collector.add_related_token_reason(data.item_type, log.token, data.reject_reason, data.related_token)
        elif isinstance(collector, LeveledCollectingTokenStatItem):
            collector.add_related_token(data.item_type, log.token, data.related_token)
        elif isinstance(collector, CollectingTokenReasonStatItem):
            collector.add_token_reason(data.item_type, log.token, data.reject_reason)
        elif isinstance(collector, CollectingReasonStatItem):
            collector.add_reason(data.item_type, data.reject_reason)
        elif isinstance(collector, CollectingTokenStatItem):
            collector.add_token(data.item_type, log.token)
        elif isinstance(collector, StatItem):
            collector.add(data.item_type)
        else:
            raise ValueError("unknown instance")

    def _can_filter(self, log):
        raise NotImplementedError()

    def _get_data(self, log):
        raise NotImplementedError()

    def inject_record(self, log):
        if self._can_filter(log):
            data = self._get_data(log)
            self.all.add(data.item_type)

            if data.judge == 'accept':
                AbstractStatFilter._add_to(self.accept, log, data)
            elif data.judge == 'reject':
                AbstractStatFilter._add_to(self.reject, log, data)
            elif data.judge == 'duplicate':
                AbstractStatFilter._add_to(self.duplicate, log, data)
            elif data.judge == 'neutral':
                AbstractStatFilter._add_to(self.neutral, log, data)
            else:
                AbstractStatFilter._add_to(self.none, log, data)

    def collect(self, seq):
        for log in seq:
            self.inject_record(log)
            yield log

    def __pretify__(self, indent):
        return _pretify(indent, {
            'total': self.all,
            'accept': self.accept,
            'reject': self.reject,
            'duplicate': self.duplicate,
            'neutral': self.neutral,
            'none': self.none
        })


class BaseFilterStats(AbstractStatFilter):
    def __init__(self, attribute, name, total, accept, reject, duplicate, neutral, none):
        super(BaseFilterStats, self).__init__(total, accept, reject, duplicate, neutral, none)
        self.attribute = attribute
        self.name = name

    def _can_filter(self, log):
        return log.is_bot and log.result and log.result.filters and hasattr(log.result.filters, self.attribute) and \
               getattr(log.result.filters, self.attribute)

    def _get_reject_reason(self, log, f):
        raise NotImplementedError()

    def _get_ignore(self, log, f):
        raise NotImplementedError()

    def _get_judge(self, log, f):
        raise NotImplementedError()

    def _get_related_token(self, log, f):
        raise NotImplementedError()

    def _get_data(self, log):
        f = getattr(log.result.filters, self.attribute)
        reject_reason = self._get_reject_reason(log, f)
        if reject_reason <= 0:
            reject_reason = log.reviewer_reject_reason_id

        if not reject_reason:
            reject_reason = 0

        ignore = self._get_ignore(log, f)
        judge = self._get_judge(log, f)

        if (not ignore) and log.result.decision:
            d = log.result.decision
            neutralized = d.judge == 'neutral' and (d.filter == self.name or judge == 'accept')
            deciding = judge == d.judge and d.filter == self.name
        else:
            neutralized = False
            deciding = False

        if ignore:
            item_type = StatItem.IGNORE
        elif neutralized:
            item_type = StatItem.NEUTRALIZED
        elif deciding:
            item_type = StatItem.DECIDING
        else:
            item_type = StatItem.MASKED

        related_token = self._get_related_token(log, f)

        return AbstractStatFilter.StatFilterData(item_type, judge, reject_reason, deciding, neutralized, related_token)


class BaseStatSimilarFilter(BaseFilterStats):
    def __init__(self):
        super(BaseStatSimilarFilter, self).__init__('similar_filter', 'SimilarFilter', StatItem(),
                                                    LeveledCollectingTokenStatItem(),
                                                    LeveledCollectingTokenReasonStatItem(), StatItem(), StatItem(),
                                                    StatItem())

    def _get_reject_reason(self, log, f):
        return f.reject_reason_id if f.reject_reason_id else 0

    def _get_ignore(self, log, f):
        return f.ignore

    def _get_judge(self, log, f):
        return f.judge

    def _get_related_token(self, log, f):
        if len(f.similar_posts) > 0:
            return f.similar_posts[0]
        else:
            return None


class BaseReviewerStats(AbstractStatFilter):
    def __init__(self, total, accept, reject, duplicate, neutral, none):
        super(BaseReviewerStats, self).__init__(total, accept, reject, duplicate, neutral, none)

    def _can_filter(self, log):
        return log.is_reviewer

    def _get_data(self, log):
        reject_reason = log.reviewer_reject_reason_id
        if not reject_reason:
            reject_reason = 0
        judge = 'reject' if reject_reason > 0 else 'accept'
        neutralized = False
        deciding = True
        item_type = StatItem.DECIDING
        related_token = None

        return AbstractStatFilter.StatFilterData(item_type, judge, reject_reason, deciding, neutralized, related_token)


class ReviewerActionedBotStats(BaseReviewerStats):
    def __init__(self, marks, bot_stats, total, accept, reject, duplicate, neutral, none, judge, action):
        super(ReviewerActionedBotStats, self).__init__(total, accept, reject, duplicate, neutral, none)
        self.marks = marks
        self.bot_stats = bot_stats
        self.judge = judge
        self.action = action

    def _can_filter(self, log):
        if not super(ReviewerActionedBotStats, self)._can_filter(log):
            return False

        if not hasattr(self.bot_stats, self.judge):
            raise ValueError("bot_stats has no %s" % self.judge)

        judge = getattr(self.bot_stats, self.judge)

        if not hasattr(judge, 'tokens'):
            raise ValueError("bot_stats.%s has no tokens" % self.judge)

        tokens = getattr(judge, 'tokens')

        if not isinstance(tokens, set) and not isinstance(tokens, BigSet):
            raise ValueError("bot_stats.%s.tokens is not a set or bigset" % self.judge)

        if log.token in tokens and (log.token not in self.marks):
            self.marks.add(log.token)

            if (self.action == 'reject' and log.reviewer_reject_reason_id <= 0) or (self.action == 'accept' and
                                                                                    log.reviewer_reject_reason_id > 0):
                return False

            return True
        else:
            return False


class StatSimilarFilter:
    def __init__(self):
        self.stats = BaseStatSimilarFilter()
        self._marks = BigSet()
        self.reviewer_rejected_accept = ReviewerActionedBotStats(self._marks, self.stats, StatItem(), StatItem(),
                                                                 CollectingReasonStatItem(), StatItem(), StatItem(),
                                                                 StatItem(), judge='accept', action='reject')
        self.reviewer_rejected_reject = ReviewerActionedBotStats(self._marks, self.stats, StatItem(), StatItem(),
                                                                 CollectingReasonStatItem(), StatItem(), StatItem(),
                                                                 StatItem(), judge='reject', action='reject')
        self.reviewer_accepted_accept = ReviewerActionedBotStats(self._marks, self.stats, StatItem(), StatItem(),
                                                                 CollectingReasonStatItem(), StatItem(), StatItem(),
                                                                 StatItem(), judge='accept', action='accept')
        self.reviewer_accepted_reject = ReviewerActionedBotStats(self._marks, self.stats, StatItem(), StatItem(),
                                                                 CollectingReasonStatItem(), StatItem(), StatItem(),
                                                                 StatItem(), judge='reject', action='accept')

    def filter_has(self, seq):
        for log in seq:
            if log.result and log.result.filters and log.result.filters.similar_filter:
                yield log

    def filter_accept(self, seq):
        for log in self.filter_has(seq):
            if log.result.filters.similar_filter.judge == 'accept':
                yield log

    def filter_deciding(self, seq):
        for log in self.filter_has(seq):
            if log.result.decision and log.result.filters.similar_filter.judge == log.result.decision.judge and \
                    log.result.decision.filter == 'SimilarFilter':
                yield log

    def filter_not_ignored(self, seq):
        for log in self.filter_has(seq):
            if not log.result.filters.similar_filter.ignore:
                yield log

    def filter_reviewer_rejected_accept(self, seq):
        for log in self.filter_not_ignored(self.filter_deciding(self.filter_accept(seq))):
            if log.is_reviewer and log.reviewer_reject_reason_id > 0:
                yield log

    def collect(self, seq):
        for log in seq:
            self.stats.inject_record(log)
            self.reviewer_rejected_accept.inject_record(log)
            self.reviewer_rejected_reject.inject_record(log)
            yield log

    def __pretify__(self, indent):
        return _pretify(indent, {
            'stats': self.stats,
            'reviewer-rejected-accept': self.reviewer_rejected_accept,
            'reviewer-rejected-reject': self.reviewer_rejected_reject,
            'reviewer-accepted-accept': self.reviewer_accepted_accept,
            'reviewer-accepted-reject': self.reviewer_accepted_reject,
        })

    def __str__(self):
        return _pretify('', self)


class DecisionStats(AbstractStatFilter):
    def __init__(self):
        super(DecisionStats, self).__init__(StatItem(), StatItem(), CollectingReasonStatItem(), StatItem(), StatItem(),
                                            StatItem())

    def _can_filter(self, log):
        return log.is_bot and log.result and log.result.decision and log.result.decision.judge

    def _get_data(self, log):
        ignore = log.result.decision.ignore
        judge = log.result.decision.judge
        neutralized = False
        reject_reason = log.result.decision.reject_reason_id
        if not reject_reason or reject_reason <= 0:
            reject_reason = log.reviewer_reject_reason_id
        if not reject_reason:
            reject_reason = 0
        deciding = True

        if ignore:
            item_type = StatItem.IGNORE
        elif neutralized:
            item_type = StatItem.NEUTRALIZED
        elif deciding:
            item_type = StatItem.DECIDING
        else:
            item_type = StatItem.MASKED

        related_token = None

        return AbstractStatFilter.StatFilterData(item_type, judge, reject_reason, deciding, neutralized, related_token)


class ReviewerStats(BaseReviewerStats):
    def __init__(self):
        super(ReviewerStats, self).__init__(StatItem(), StatItem(), CollectingReasonStatItem(), StatItem(), StatItem(),
                                            StatItem())


class StatFilter:
    def __init__(self):
        self.bot_stats = DecisionStats()
        self.reviewer_stats = ReviewerStats()

    def collect(self, seq):
        for log in seq:
            self.bot_stats.inject_record(log)
            self.reviewer_stats.inject_record(log)
            yield log

    def __pretify__(self, indent):
        return _pretify(indent, {
            'bot_stats': self.bot_stats,
            'reviewer_stats': self.reviewer_stats,
        })

    def __str__(self):
        return _pretify('', self)
