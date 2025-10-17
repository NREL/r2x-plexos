"""Utils for parsing plexos XMLs."""


def apply_action(base_value: float, new_value: float, action: str | None) -> float:
    """Apply a PLEXOS action operation to combine values.

    Parameters
    ----------
    base_value : float
        The current/base value
    new_value : float
        The new value to apply
    action : str | None
        The action to perform: "=", "*", "+", "-", "/"

    Returns
    -------
    float
        The result of applying the action
    """
    if action == "*":
        return base_value * new_value
    elif action == "+":
        return base_value + new_value
    elif action == "-":
        return base_value - new_value
    elif action == "/" and new_value != 0:
        return base_value / new_value
    else:  # "=" or unknown - just return new value
        return new_value
