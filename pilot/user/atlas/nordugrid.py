#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2019

import re
from xml.dom import minidom
from xml.etree import ElementTree

import logging
logger = logging.getLogger(__name__)


class XMLDictionary(object):
    """
    This is a helper class that is used to create the dictionary which is converted to the special XML files for
    Nordugrid pilots.
    Example dictionary:
      dictionary = { "outfiles": [ { "file": { "surl": "some_surl", "size": "123", "ad32": "aaaaaaa",
                                               "guid": "ababa22", "lfn": "some_lfn", "dataset": "some_dataset",
                                               "date": "11/11/11" } },
                                    {}, {}, ..
                                 ]
                    }

    Usage:
      xmldic = XMLDictionary()
      xmldic.add_to_list({"surl": "some_surl1", "size": "123", "ad32": "aaaaaaa", "guid": "ababa22", "lfn": "some_lfn",
                          "dataset": "some_dataset", "date": "11/11/11"})
      dictionary = xmldic.get_dictionary()
    """

    _dictionary = None

    def __init__(self, rootname="outfiles"):
        """
        Standard init function.
        :param rootname: name of the root key. There is only one root key in the Nordugrid XML file ('outfiles').
        """
        self._dictionary = {}
        self._dictionary[rootname] = []

    def add_to_list(self, dictionary, rootname="outfiles", itemname="file"):
        """
        Add dictionary to itemname key. See example in class header.
        :param dictionary: dictionary to add to itemname key.
        :param rootname: name of the root key. There is only one root key in the Nordugrid XML file ('outfiles').
        :param itemname: name of the item key. In the Nordugrid XML it should be called 'file'.
        :return:
        """
        if type(self._dictionary) == dict:
            if type(self._dictionary[rootname]) == list:
                _dic = {itemname: dictionary}
                self._dictionary[rootname].append(_dic)
            else:
                pass
        else:
            logger.info("not a dictionary: %s" % str(self._dictionary))

    def get_dictionary(self):
        """
        Return the dictionary to be converted to XML.
        It should be populated with the dictionary added to it in add_to_list().
        :return: dictionary
        """
        return self._dictionary


def convert_to_xml(dictionary):
    """
    Convert a dictionary to XML.
    The dictionary is expected to follow the Nordugrid format. See the XMLDictionary helper class.

    Example of XML (OutputFiles.xml):

    <?xml version="1.0" ?>
    <outfiles>
    <file>
      <ad32>aaaaaaa</ad32>
      <surl>some_surl1</surl>
      <lfn>some_lfn</lfn>
      <dataset>some_dataset</dataset>
      <date>11/11/11</date>
      <guid>ababa22</guid>
      <size>123</size>
    </file>
    </outfiles>

    :param dictionary: dictionary created with XMLDictionary.
    :return: xml (pretty printed for python >= 2.7 - for older python, use the convert_to_prettyprint() function).
    """

    failed = False

    single_file_tag = list(dictionary.keys())  # Python 2/3
    if len(single_file_tag) != 1:
        logger.warning("unexpected format - expected single entry, got %d entries" % len(single_file_tag))
        logger.warning('dictionary = %s' % str(dictionary))
        return None

    file_tag = single_file_tag[0]
    root = ElementTree.Element(file_tag)

    file_list = dictionary[file_tag]
    if type(file_list) == list:
        for file_entry in file_list:
            if type(file_entry) == dict and len(file_entry) == 1:
                single_entry = list(file_entry.keys())[0]  # Python 2/3

                # add the 'file' element
                file_element = ElementTree.Element(single_entry)
                root.append(file_element)

                file_dictionary = file_entry[single_entry]
                if type(file_dictionary) == dict:
                    for dictionary_entry in list(file_dictionary.keys()):  # Python 2/3
                        # convert all entries to xml elements
                        entry = ElementTree.SubElement(file_element, dictionary_entry)
                        entry.text = file_dictionary[dictionary_entry]
                else:
                    logger.warning("unexpected format - expected a dictionary, got %s" % file_dictionary)
                    failed = True
            else:
                logger.warning("unexpected format - expected a length 1 dictionary, got %s" % file_entry)
                failed = True
    else:
        logger.warning("unexpected format - expected a list, got %s" % file_list)
        failed = True

    if failed:
        return None

    # generate pretty print
    return minidom.parseString(ElementTree.tostring(root)).toprettyxml(indent="   ")


def convert_to_prettyprint(xmlstr):
    """
    Convert XML to pretty print for older python versions (< 2.7).
    :param xmlstr: input XML string
    :return: XML string (pretty printed)
    """

    text_re = re.compile(r'>\n\s+([^<>\s].*?)\n\s+</', re.DOTALL)  # Python 3 (added r)
    return text_re.sub(r'>\g<1></', xmlstr)  # Python 3 (added r)
