class Log4J:
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        root_class = "saurav.code.spark.examples"
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        self.logger = log4j.LogManager.getLogger(root_class + "." + app_name)

    def warn(self, message):
        return self.logger.warn(message)

    def error(self, message):
        return self.logger.error(message)

    def debug(self, message):
        return self.logger.debug(message)

    def info(self, message):
        return self.logger.info(message)
