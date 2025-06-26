"""Database operation decorators for consistent error handling."""

from collections.abc import Awaitable, Callable
from functools import wraps
import inspect
from types import NoneType, UnionType
from typing import Any, Union, cast, get_args, get_origin, get_type_hints

from sqlalchemy.exc import SQLAlchemyError

from ..exceptions import DatabaseOperationError


def _resolve_single_annotation(cls: type, attr_name: str) -> Any:
    """Resolve a single type annotation.

    Args:
        cls: The class containing the annotation.
        attr_name: The attribute name to resolve.

    Returns:
        The resolved type annotation.

    Raises:
        TypeError: If the annotation can't be resolved.
    """
    if not hasattr(cls, "__annotations__"):
        raise TypeError(f"Class {cls.__name__} has no annotations")

    raw_annotation = cls.__annotations__.get(attr_name)
    if raw_annotation is None:
        raise TypeError(
            f"Class {cls.__name__} has no annotation for attribute '{attr_name}'"
        )

    # If it's already a resolved type, return it
    if not isinstance(raw_annotation, str):
        return raw_annotation

    # Try to resolve string annotation in the class's module context
    try:
        import sys

        module = sys.modules.get(cls.__module__)
        if module is None:
            raise TypeError(
                f"Cannot find module {cls.__module__} for class {cls.__name__}"
            )

        # Simple eval in module context - only for non-forward references
        if '"' not in raw_annotation and "'" not in raw_annotation:
            return eval(raw_annotation, module.__dict__)
        else:
            raise TypeError(
                f"Forward reference '{raw_annotation}' in {cls.__name__}.{attr_name} cannot be resolved"
            )
    except (NameError, AttributeError, SyntaxError) as e:
        raise TypeError(
            f"Failed to resolve annotation '{raw_annotation}' in {cls.__name__}.{attr_name}: {e}"
        ) from e


def _validate_id_path(
    func_name: str,
    sig: inspect.Signature,
    resolved_hints: dict[str, Any],
    path: str,
) -> None:
    """Inspect a function's signature and type hints to validate a dotted attribute path.

    This function is called at decoration time to fail fast if the path is invalid.

    Args:
        func_name: The name of the function being decorated (for error messages).
        sig: The signature of the function.
        resolved_hints: The resolved type hints for the function.
        path: The dotted attribute path to validate (e.g., "feed.id").

    Raises:
        TypeError: If the path is invalid, a parameter is missing, or a type
                   is incorrect or missing.
    """
    path_parts = path.split(".")
    base_param_name = path_parts[0]
    attr_path = path_parts[1:]

    if base_param_name not in sig.parameters:
        raise TypeError(
            f"Decorator on '{func_name}' specifies path '{path}', "
            f"but the function has no parameter named '{base_param_name}'."
        )

    current_type = resolved_hints.get(base_param_name)
    for i, attr_name in enumerate(attr_path):
        current_path_str = ".".join(path_parts[: i + 1])
        if current_type is None or current_type is inspect.Parameter.empty:
            raise TypeError(
                f"In path '{path}', part '{path_parts[i]}' has no type annotation on '{func_name}'."
            )

        # Handle `T | None` by unwrapping the type T
        if get_origin(current_type) in (Union, UnionType):
            non_none_types = [
                arg for arg in get_args(current_type) if arg is not NoneType
            ]
            if len(non_none_types) == 1:
                current_type = non_none_types[0]
            else:
                raise TypeError(
                    f"Path '{current_path_str}' contains an unsupported Union or Generic type: {current_type}"
                )

        try:
            current_type = _resolve_single_annotation(current_type, attr_name)
        except TypeError as e:
            raise TypeError(
                f"In path '{path}', cannot resolve attribute '{attr_name}' on type "
                f"'{getattr(current_type, '__name__', current_type)}': {e}"
            ) from e

    # Final check: the type of the final attribute must be str or str | None
    is_valid_type = False
    if current_type is str:
        is_valid_type = True
    elif get_origin(current_type) in (Union, UnionType):
        args = get_args(current_type)
        if len(args) == 2 and str in args and NoneType in args:
            is_valid_type = True

    if not is_valid_type:
        raise TypeError(
            f"The final attribute '{path_parts[-1]}' in path '{path}' must be "
            f"typed as 'str' or 'str | None', but found '{current_type}' in '{func_name}'."
        )


def _extract_value_from_path(
    bound_args: inspect.BoundArguments, path: str
) -> str | None:
    """Extract a value from bound arguments at runtime by following a dotted path.

    Args:
        bound_args: The bound arguments from the decorated function call.
        path: The dotted attribute path to extract (e.g., "feed.id").

    Returns:
        The extracted string value, or None if not found.
    """
    path_parts = path.split(".")
    base_param_name = path_parts[0]
    attr_path = path_parts[1:]

    current_value = bound_args.arguments.get(base_param_name)
    if current_value is not None:
        for attr in attr_path:
            current_value = getattr(current_value, attr, None)
            if current_value is None:
                break
    return cast(str | None, current_value)


def _base_db_error_handler[**P, T](
    operation: str,
    id_paths: dict[str, str] | None = None,
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """A generalized, internal decorator for handling database errors.

    It validates and extracts multiple IDs based on a dictionary of paths and
    injects them into the raised DatabaseOperationError.

    Args:
        operation: Description of the operation for error messages.
        id_paths: A dictionary mapping the desired keyword in the final error
                  (e.g., "feed_id") to its extraction path (e.g., "feed.id").

    Returns:
        A decorator that wraps a function in SQLAlchemyError handling.
    """
    if id_paths is None:
        id_paths = {}

    def decorator(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        # --- Validation at decoration time ---
        try:
            sig = inspect.signature(func)
            resolved_hints = get_type_hints(func)
        except (TypeError, ValueError) as e:
            raise TypeError(
                f"Could not inspect the signature of {func.__name__}."
            ) from e

        for path in id_paths.values():
            _validate_id_path(func.__name__, sig, resolved_hints, path)

        @wraps(func)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            # --- Extraction and Error Handling at runtime ---
            extracted_ids: dict[str, str | None] = {}
            if id_paths:
                try:
                    bound_args = sig.bind(*args, **kwargs)
                    bound_args.apply_defaults()
                    for id_name, path in id_paths.items():
                        extracted_ids[id_name] = _extract_value_from_path(
                            bound_args, path
                        )
                except (TypeError, ValueError, AttributeError) as e:
                    raise ValueError(
                        f"Failed to extract IDs {id_paths} in {func.__name__}"
                    ) from e

            try:
                return await func(*args, **kwargs)
            except SQLAlchemyError as e:
                raise DatabaseOperationError(
                    f"Failed to {operation}", **extracted_ids
                ) from e

        return wrapper

    return decorator


def handle_db_errors[**P, T](
    operation: str,
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Decorator for generic database operations without specific context.

    Catches SQLAlchemyError and raises DatabaseOperationError.
    """
    return _base_db_error_handler(operation=operation)


def handle_feed_db_errors[**P, T](
    operation: str,
    feed_id_from: str = "feed_id",
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Decorator for database operations involving a feed.

    Extracts feed_id for error reporting and raises DatabaseOperationError.
    """
    id_paths = {"feed_id": feed_id_from}
    return _base_db_error_handler(operation=operation, id_paths=id_paths)


def handle_download_db_errors[**P, T](
    operation: str,
    feed_id_from: str = "feed_id",
    download_id_from: str = "download_id",
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Decorator for database operations involving a feed and download.

    Extracts feed_id and download_id for error reporting and raises DatabaseOperationError.
    """
    id_paths = {
        "feed_id": feed_id_from,
        "download_id": download_id_from,
    }
    return _base_db_error_handler(operation=operation, id_paths=id_paths)
