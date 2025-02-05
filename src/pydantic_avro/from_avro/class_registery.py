class ClassRegistry:
    """Singleton class to store generated Pydantic classes."""

    _instance = None
    _classes: dict = {}

    def __new__(cls):
        """Singleton implementation."""
        if cls._instance is None:
            cls._instance = super(ClassRegistry, cls).__new__(cls)
            cls._instance._classes = {}
        return cls._instance

    def add_class(self, name: str, class_def: str):
        """Add a class to the registry."""
        formatted_name = self.format_class_name(name)
        formatted_class_def = self.replace_class_def_name(name, class_def)
        self._classes[formatted_name] = formatted_class_def

    @property
    def classes(self) -> dict:
        """Get all classes in the registry."""
        return self._classes

    def has_class(self, name: str) -> bool:
        """Check if a class is in the registry."""
        return name in self._classes.keys()

    def clear(self):
        """Clear all classes from the registry."""
        self._classes.clear()

    def format_class_name(self, name: str) -> str:
        """Format the class name to be Pythonic."""
        return name.replace("_", "")

    def replace_class_def_name(self, class_name: str, class_def: str) -> str:
        """Format the class definition to be Pythonic."""
        formatted_class_name = self.format_class_name(class_name)
        return class_def.replace(class_name, formatted_class_name)
