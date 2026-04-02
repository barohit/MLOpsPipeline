class BaseModelTrainingProcess:
    def __init__(self, data_source, model_registry):
        self.data_source = data_source
        self.model_registry = model_registry

    def run(self):
        raise NotImplementedError