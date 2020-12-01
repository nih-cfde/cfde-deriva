
class CfdeError (Exception):
    """Super-class for all CFDE-deriva errors."""
    pass

class UnknownDccId (CfdeError):
    """A supplied DCC id is not known by the registry."""
    pass

class Forbidden (CfdeError):
    """The requested operation is forbidden by policy."""
    pass

class RegistrationError (CfdeError):
    """The submission was not entered into the registry."""
    pass

