class SignalRegistry:

  _registry = {}

  @classmethod
  def register(cls, name, generator_class):
    cls._registry[name] = generator_class

  @classmethod
  def get_generator(cls, name):
    if name not in cls._registry:
      raise ValueError(f"Signal '{name}' not found in registry")
    
    return cls._registry[name]