import boto3

import logging

logger = logging.getLogger("turkic.api")

class Server(object):
    def __init__(self, signature, accesskey, localhost, sandbox = False):
        self.signature = signature
        self.accesskey = accesskey
        self.localhost = localhost
        self.sandbox = sandbox

        if self.sandbox:
            endpoint_url = "https://mturk-requester-sandbox.us-east-1.amazonaws.com"
        else:
            endpoint_url = "https://mturk-requester.us-east-1.amazonaws.com"

        self.client = boto3.client('mturk',
                                   region_name='us-east-1',
                                   endpoint_url=endpoint_url,
                                   aws_access_key_id=self.accesskey,
                                   aws_secret_access_key=self.signature)

    def request(self, operation, parameters = {}):
        """
        Sends the request to the Turk server and returns a response object.
        """

        logger.info("Request to MTurk: {0}".format(operation))
        for paramk, paramv in parameters.items():
            logger.debug("  {0}: {1}".format(paramk, paramv))

        if operation == "CreateHIT":
            data = self.client.create_hit(**parameters)
        if operation == "DisableHIT":
            self.client.update_expiration_for_hit(ExpireAt=0, **parameters)
            data = self.client.delete_hit(**parameters)
        if operation == "ApproveAssignment":
            data = self.client.approve_assignment(**parameters)
        if operation == "RejectAssignment":
            data = self.client.reject_assignment(**parameters)
        if operation == "GrantBonus":
            data = self.client.send_bonus(**parameters)
        if operation == "BlockWorker":
            data = self.client.create_worker_block(**parameters)
        if operation == "UnblockWorker":
            data = self.client.delete_worker_block(**parameters)
        if operation == "NotifyWorkers":
            data = self.client.notify_workers(**parameters)
        if operation == "GetAccountBalance":
            data = self.client.get_account_balance()

        response = Response(operation, data)
        return response

    def createhit(self, title, description, page, amount, duration,
        lifetime, keywords = "", autoapprove = 604800, height = 650,
        minapprovedpercent = None, minapprovedamount = None, countrycode = None):
        """
        Creates a HIT on Mechanical Turk.
        
        If successful, returns a Response object that has fields:
            hit_id          The HIT ID
            hit_type_id     The HIT group ID

        If unsuccessful, a CommunicationError is raised with a message
        describing the failure.
        """
        r = {"Title": title,
            "Description": description,
            "Keywords": keywords,
            "Reward": str(amount),
            "AssignmentDurationInSeconds": duration,
            "AutoApprovalDelayInSeconds": autoapprove,
            "LifetimeInSeconds": lifetime}

        qual = []

        if minapprovedpercent:
            qual.append({
                "QualificationTypeId": "000000000000000000L0",
                "Comparator": "GreaterThanOrEqualTo",
                "IntegerValues": [minapprovedpercent],
            })

        if minapprovedamount:
            qual.append({
                "QualificationTypeId": "00000000000000000040",
                "Comparator": "GreaterThanOrEqualTo",
                "IntegerValues": [minapprovedamount],
            })

        if countrycode:
            qual.append({
                "QualificationTypeId": "00000000000000000071",
                "Comparator": "EqualTo",
                "LocaleValues": [{
                    "Country": countrycode
                }],
            })

        if qual:
            r["QualificationRequirements"] = qual

        r["Question"] = ("<ExternalQuestion xmlns=\"http://mechanicalturk"
                         ".amazonaws.com/AWSMechanicalTurkDataSchemas/"
                         "2006-07-14/ExternalQuestion.xsd\">"
                         "<ExternalURL>{0}/{1}</ExternalURL>"
                         "<FrameHeight>{2}</FrameHeight>"
                         "</ExternalQuestion>").format(self.localhost,
                                                       page, height)

        r = self.request("CreateHIT", r);
        r.store("HIT/HITId", "hitid")
        r.store("HIT/HITTypeId", "hittypeid")
        return r
    
    def disable(self, hitid):
        """
        Disables the HIT from the MTurk service.
        """
        r = self.request("DisableHIT", {"HITId": hitid})
        return r

    def purge(self):
        """
        Disables all the HITs on the MTurk server. Useful for debugging.
        """
        raise NotImplementedError()

    def accept(self, assignmentid, feedback = ""):
        """
        Accepts the assignment and pays the worker.
        """
        r = self.request("ApproveAssignment",
                         {"AssignmentId": assignmentid,
                          "RequesterFeedback": feedback})
        return r

    def reject(self, assignmentid, feedback = ""):
        """
        Rejects the assignment and does not pay the worker.
        """
        r = self.request("RejectAssignment",
                         {"AssignmentId": assignmentid,
                          "RequesterFeedback": feedback})
        return r

    def bonus(self, workerid, assignmentid, amount, feedback = ""):
        """
        Grants a bonus to a worker for an assignment.
        """
        r = self.request("GrantBonus",
            {"WorkerId": workerid,
             "AssignmentId": assignmentid,
             "BonusAmount": str(amount),
             "Reason": feedback});
        return r

    def block(self, workerid, reason = ""):
        """
        Blocks the worker from working on any of our HITs.
        """
        r = self.request("BlockWorker", {"WorkerId": workerid,
                                         "Reason": reason})
        return r

    def unblock(self, workerid, reason = ""):
        """
        Unblocks the worker and allows him to work for us again.
        """
        r = self.request("UnblockWorker", {"WorkerId": workerid,
                                           "Reason": reason})
        return r

    def email(self, workerid, subject, message):
        """
        Sends an email to the worker.
        """
        r = self.request("NotifyWorkers", {"Subject": subject,
                                           "MessageText": message,
                                           "WorkerIds": [workerid]})
        return r

    def getstatistic(self, statistic, type, timeperiod = "LifeToDate"):
        """
        Returns the total reward payout.
        """
        raise NotImplementedError()

    @property
    def balance(self):
        """
        Returns a response object with the available balance in the amount
        attribute.
        """
        r = self.request("GetAccountBalance")
        r.store("AvailableBalance", "amount", float)
        return r.amount

    @property
    def rewardpayout(self):
        """
        Returns the total reward payout.
        """
        raise NotImplementedError()

    @property
    def approvalpercentage(self):
        """
        Returns the percent of assignments approved.
        """
        raise NotImplementedError()

    @property
    def feepayout(self):
        """
        Returns how much we paid to Amazon in fees.
        """
        raise NotImplementedError()

    @property
    def numcreated(self):
        """
        Returns the total number of HITs created.
        """
        raise NotImplementedError()

class Response(object):
    """
    A generic response from the MTurk server.
    """
    def __init__(self, operation, clientresponse):
        self.operation = operation
        self.clientresponse = clientresponse
        self.values = {}

    def find(self, resp, path, default=None):
        """
        Finds value in a nested dictionary.
        """
        keys = path.split("/")
        val = None
        for key in keys:
            if val:
                val = val.get(key, default)
            else:
                val = resp.get(key, default)
            if not val:
                break
        return val

    def store(self, path, name, type = str):
        """
        Stores the text at path into the attribute name.
        """
        val = self.find(self.clientresponse, path)
        if val is None:
            raise CommunicationError("Client response malformed "
                "(cannot find {0})".format(path), self)
        self.values[name] = type(val.strip())

    def __getattr__(self, name):
        """
        Used to lookup attributes.
        """
        if name not in self.values:
            raise AttributeError("{0} is not stored".format(name))
        return self.values[name]

class CommunicationError(Exception):
    """
    The error raised due to a communication failure with MTurk.
    """
    def __init__(self, error, response):
        self.error = error
        self.response = response

    def __str__(self):
        return self.error

try:
    import config
except ImportError:
    pass
else:
    server = Server(config.signature,
                    config.accesskey,
                    config.localhost,
                    config.sandbox)
