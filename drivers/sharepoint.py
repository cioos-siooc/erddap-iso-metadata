from zeep import Client
from zeep.transports import Transport
from requests_ntlm import HttpNtlmAuth
from requests import Session
from lxml import etree, objectify

import re
import yaml

class SharePoint:
    def load_data(config):
        pass
