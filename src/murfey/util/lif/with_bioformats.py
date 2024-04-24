"""
LIF image handling functions using python-bioformats as a base.

Sadly, python-javabridge has been abandoned, and cannot be installed via pip on Pythons >3.10.
Python-bioformats will have to be abandoned as an option.
"""

# import time as tm  # Prevent ambiguity with time as-defined in functions below
# import xml.etree.ElementTree as ET
# from multiprocessing import Process, Queue
# from pathlib import Path
# from typing import Callable

# import bioformats as bf
# import javabridge as jb  # javabridge can't work with Python > 3.10


# def _init_logger():
#     """
#     This is so that Javabridge doesn't spill out a lot of DEBUG/WARNING messages during
#     runtime.
#     Source: https://github.com/pskeshu/microscoper/blob/master/microscoper/io.py#L141-L162

#     Valid logging options: TRACE, DEBUG, INFO, WARN, ERROR, OFF, ALL
#     Source: https://logback.qos.ch/manual/architecture.html
#     """

#     rootLoggerName = jb.get_static_field(
#         "org/slf4j/Logger", "ROOT_LOGGER_NAME", "Ljava/lang/String;"
#     )

#     rootLogger = jb.static_call(
#         "org/slf4j/LoggerFactory",
#         "getLogger",
#         "(Ljava/lang/String;)Lorg/slf4j/Logger;",
#         rootLoggerName,
#     )

#     logLevel = jb.get_static_field(
#         "ch/qos/logback/classic/Level",
#         "ERROR",  # Show only error messages or worse
#         "Lch/qos/logback/classic/Level;",
#     )

#     jb.call(rootLogger, "setLevel", "(Lch/qos/logback/classic/Level;)V", logLevel)
#     return None


# def _run_as_separate_process(
#     function: Callable, args=list  # List of arguments the function takes, IN ORDER
# ):
#     """
#     Run the function as its own separate process. Currently used for handling functions
#     that make use of Java virtual machines, which cannot be started again after they
#     have been stopped in a Python instance.
#     """
#     # Create a queue object to pass to the process
#     queue: Queue = Queue()

#     # Run functions that need JVM instances as a separate process
#     p = Process(
#         target=function, args=(*args, queue)  # Process takes arguments as a tuple
#     )
#     p.start()

#     # Extract the result from the function
#     results = queue.get()
#     p.join()

#     return results


# def _get_xml_string(file: Path, queue: Queue):  # multiprocessing queue
#     # Start Java virtual machine
#     jb.start_vm(class_path=bf.JARS, run_headless=True)
#     _init_logger()

#     # Get OME-XML string from file
#     xml_string = bf.get_omexml_metadata(path=str(file))
#     print("Loaded OME-XML metadata from file")

#     # Kill virtual machine
#     jb.kill_vm()

#     # Add result to queue
#     queue.put(xml_string)
#     return xml_string


# def get_xml_string(file: Path):
#     xml_string = _run_as_separate_process(function=_get_xml_string, args=[file])
#     return xml_string


# def write_as_raw_xml(xml_string: str, save_file: Path):
#     # Write raw xml to file
#     if save_file.exists():
#         print("XML file already exists")
#         pass
#     else:
#         with open(save_file, mode="w", encoding="utf-8") as log_file:
#             log_file.writelines(xml_string)
#         log_file.close()
#         print("Wrote raw OME-XML metadata to XML file")

#     return save_file


# def convert_to_xml_tree(xml_string: str):
#     # Convert to ElementTree
#     tree = ET.ElementTree(ET.fromstring(xml_string))
#     print("Created ElementTree successfully")

#     # Add indent to XML file
#     ET.indent(tree, space="\t", level=0)

#     return tree


# def write_as_pretty_xml(xml_tree: ET.ElementTree, save_file: Path):
#     # Write out metadata contents in a formatted structure
#     if save_file.exists():
#         print("XML file already exists")
#         pass
#     else:
#         xml_tree.write(save_file, encoding="utf-8")
#         print("Wrote formatted OME-XML metadata to XML file")

#     return save_file


# def extract_xml_metadata(file: Path):
#     # Convert OME-XML metadata
#     xml_string = get_xml_string(file=file)
#     xml_tree = convert_to_xml_tree(xml_string=xml_string)
#     save_file = file.parent.joinpath(file.stem + ".xml")
#     write_as_pretty_xml(xml_tree=xml_tree, file=file)

#     return xml_tree


# def main():
#     # Start the stopwatch
#     time_start = tm.time_ns()

#     # Define test directory to extract data from
#     test_repo = Path(
#         "/dls/ebic/data/staff-scratch/tieneupin/projects/murfey-clem/test-data/nt26538-160/raw"
#     )  # Create as Path object
#     print(f"Test repository is {test_repo}")

#     file_ext = ".lif"  # Look only for .lif files

#     # Get list of files
#     file_list = list(test_repo.glob("*" + file_ext))  # Search via glob and convert to list object
#     file_list.sort()  # Sort in alphabetical order

#     for f in range(len(file_list)):
#         if not f == 1:  # Select one file to work with
#             continue

#         file = file_list[f]

#         # Extract data
#         xml_tree = extract_xml_metadata(file=file)  # Get and save metadata

#     # Stop the stopwatch
#     time_stop = tm.time_ns()
#     # Report time taken
#     time_diff = time_stop - time_start  # In ns
#     print(f"Time to completion was {round(time_diff * 10**-9, 2)} s")


# # Run only if opened as a file
# if __name__ == "__main__":
#     main()
