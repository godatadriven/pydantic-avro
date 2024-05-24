class ClassRegistry:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ClassRegistry, cls).__new__(cls)
            cls._instance._classes = {}
        return cls._instance

    def add_class(self, name: str, class_def: str):
        self._classes[name] = class_def

    def get_class(self, name: str) -> str:
        if name not in self._classes:
            raise KeyError(f"Class {name} not found in registry")
        return self._classes[name]

    @property
    def classes(self) -> dict:
        return self._classes

    def has_class(self, name: str) -> bool:
        return name in self._classes.keys()
