"""
Pilot Information component

A set of low-level information providers to aggregate, prioritize (overwrite),
hide dependency to external storages and expose (queue, site, storage, etc) details
in a unified structured way to all Pilot modules by providing high-level API

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""


from .infoservice import InfoService
from .queuedata import QueueData
