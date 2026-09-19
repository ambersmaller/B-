class ContextRecord:
    """上下文记录器"""

    _instance = None

    def __new__(cls, max_messages: int = 15):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.max_messages = max_messages * 2
            cls._instance.message_dict = {}
        return cls._instance

    def put_message(self, sender: str, message: str, is_ai: bool):
        """插入消息"""
        if sender not in self.message_dict:
            self.message_dict[sender] = []

        if len(self.message_dict[sender]) >= self.max_messages:
            self.message_dict[sender].pop(0)

        self.message_dict[sender].append(
            {"role": "assistant" if is_ai else "user", "content": f"{message}"}
        )

    def get_messages(self, sender: str) -> list[dict]:
        """获取消息"""
        return self.message_dict.get(sender, [])
