import os

class TransactionManager:

    def __init__(self):
        self._last_success_file = os.open('resources/transaction/last_success.gtid', os.O_CREAT | os.O_RDWR)
        self._last_request_file = os.open('resources/transaction/last_request.gtid', os.O_CREAT | os.O_RDWR)
        if (os.stat(self._last_success_file).st_size > 0):
            self.last_success_id = self._last_success_file.read()
        if (os.stat(self._last_request_file).st_size > 0):
            self.last_request_sent_id = self._last_request_file.read()

    def get_last_success_id(self):
        return self.last_success_id

    def get_last_request_sent_id(self):
        return self.last_request_sent_id

    def is_synchronized(self):
        return self.get_last_request_sent_id() == self.get_last_success_id()

    def write_last_success_gtid(self, id):
        self._last_success_file.seek(0)
        self._last_success_file.write(id)

    def write_last_request_sent(self, id):
        self._last_request_file.seek(0)
        self._last_request_file.write(id)

    def __del__(self):
        os.close(self._last_request_file)
        os.close(self._last_success_file)
