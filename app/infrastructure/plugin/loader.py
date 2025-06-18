import os
import importlib.util
from typing import List, Tuple, Type
from loguru import logger

from sdk.ghost_downloader_sdk.interfaces import IFeaturePack

def loadFeaturePackClassesFromDirectory(directory: str) -> List[Tuple[str, Type[IFeaturePack]]]:
    """
    Scans a specified directory for valid feature packs, dynamically loads them,
    and returns a list of instantiated IFeaturePack objects.

    A valid feature pack is a directory containing an `__init__.py` file.

    Args:
        directory: The absolute or relative path to the features' directory.

    Returns:
        A list of successfully loaded IFeaturePack instances.
    """
    loadedPackClasses: List[Tuple[str, Type[IFeaturePack]]] = []

    if not os.path.isdir(directory):
        logger.warning(f"Feature pack directory not found, creating it: '{directory}'")
        try:
            os.makedirs(directory)
        except OSError as e:
            logger.error(f"Could not create feature pack directory '{directory}': {e}")
            return loadedPackClasses

    logger.info(f"Scanning for feature packs in: '{os.path.abspath(directory)}'")

    for itemName in os.listdir(directory):
        itemPath = os.path.join(directory, itemName)
        if os.path.isdir(itemPath) and '__init__.py' in os.listdir(itemPath):
            try:
                moduleName = itemName
                packClass = _findPackClassInModule(moduleName, itemPath)
                if packClass:
                    loadedPackClasses.append((moduleName, packClass))
            except Exception as e:
                logger.error(f"Failed to load feature pack from directory '{itemName}': {e}", exc_info=True)

    return loadedPackClasses


def _findPackClassInModule(moduleName: str, modulePath: str) -> IFeaturePack | None:
    """
    Dynamically imports a feature pack module from a given path and
    instantiates the IFeaturePack implementation within it.

    This function defines the contract for how a feature pack module should
    expose its main class.

    Args:
        moduleName: The name to assign to the dynamically loaded module.
        modulePath: The path to the directory of the module.

    Returns:
        An instance of IFeaturePack if found, otherwise None.
    """
    initFilePath = os.path.join(modulePath, '__init__.py')

    try:
        # Create a module spec from the file path.
        spec = importlib.util.spec_from_file_location(moduleName, initFilePath)
        if not spec or not spec.loader:
            raise ImportError(f"Could not create module spec for '{moduleName}' at '{initFilePath}'")

        # Create a new module object based on the spec.
        module = importlib.util.module_from_spec(spec)

        # Execute the module's code in the new module object's namespace.
        spec.loader.exec_module(module)
    except Exception as e:
        logger.error(f"Error executing module code for '{moduleName}': {e}")
        return None

    # --- Strategy to find the IFeaturePack implementation ---

    for attrName in dir(module):
        if attrName.startswith('_'):
            continue

        attr = getattr(module, attrName)
        if isinstance(attr, type) and issubclass(attr, IFeaturePack) and attr is not IFeaturePack:
            logger.debug(f"Found IFeaturePack class '{attrName}' in '{moduleName}'.")
            return attr

    logger.warning(f"No IFeaturePack implementation class found in module: '{moduleName}'")
    return None
