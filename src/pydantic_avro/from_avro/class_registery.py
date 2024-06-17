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
        self._classes[name] = class_def

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
