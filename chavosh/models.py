
class Decision:
    def __init__(self):
        self.judge = None
        self.ignore = False
        self.debug = False
        self.similar_posts = []
        self.filter = None
        self.review_reason = None
        self.reject_reason_id = None

    def __str__(self):
        return repr({
            'judge': self.judge,
            'ignore': self.ignore,
            'debug': self.debug,
            'similar_posts': str(self.similar_posts),
            'filter': self.filter,
            'review_reason': self.review_reason,
            'reject_reason_id': self.reject_reason_id
        })


class SimilarFilter:
    def __init__(self):
        self.reject_reason_id = None
        self.ignore = False
        self.judge = None
        self.similar_posts = []
        self.review_reason = None
        self.debug = False

    def __str__(self):
        return repr({
            'reject_reason_id': self.reject_reason_id,
            'ignore': self.ignore,
            'judge': self.judge,
            'similar_posts': str(self.similar_posts),
            'review_reason': self.review_reason,
            'debug': self.debug
        })


class Filters:
    def __init__(self):
        self.similar_filter = None

    def __str__(self):
        return repr({
            'similar_filter': str(self.similar_filter)
        })


class Result:
    def __init__(self):
        self.decision = None
        self.filters = Filters()

    def __str__(self):
        return repr({
            'decision': str(self.decision),
            'filters': str(self.filters)
        })

