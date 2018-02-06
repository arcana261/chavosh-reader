

class Log:
    def __init__(self):
        self.result = None
        self.time = None
        self.token = None
        self.reviewer_reject_reason_id = None
        self.is_system = False
        self.is_bot = False
        self.similar_post_tokens = []
        self.is_reviewer = False
        self.lines = []

    def __str__(self):
        return repr({
            'result': str(self.result),
            'time': str(self.time),
            'token': str(self.token),
            'review_reject_reason_id': str(self.reviewer_reject_reason_id),
            'is_system': str(self.is_system),
            'is_bot': str(self.is_bot),
            'similar_post_tokens': str(self.similar_post_tokens),
            'is_reviewer': str(self.is_reviewer)
        })


