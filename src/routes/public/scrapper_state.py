class Scrapper:
    is_running_task = False

    @staticmethod
    def getTaskState():
        return Scrapper.is_running_task
    
    @staticmethod
    def setTaskState(state):
        Scrapper.is_running_task = state