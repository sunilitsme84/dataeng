class Log4j(object):

    def __init__(self,spark):
        #get spark app details with which to prefix all messages

        log4j = spark._jvm.org.apache.log4j
        self.logger = log4j.LogManager.getLogger("retail_analysis")

        def error(self,message):
            """Log an error.

            :param: Error message to write to log
            :return: None
            """
            self.logger.error(message)
