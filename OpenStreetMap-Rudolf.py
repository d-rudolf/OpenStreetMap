# -*- coding: utf-8 -*-
"""
Created on Tue Feb 28 09:35:50 2017

@author: D.Rudolf
"""

import xml.etree.cElementTree as ET
import re
import pprint
import codecs
import json

OSMFILE = "map_Duesseldorf_Neuss.osm"

problemchars_streetname = re.compile(r'[=\+\&<>;\"\?%#$@\,\t\r\n]', re.IGNORECASE)
problemchars_phone = re.compile(r'[=\/&<>;\'"\?%#$@\,\.\t\r\n]', re.IGNORECASE)
phone_re = re.compile(r"\+49|0049")
phone_re_0049 = re.compile(r"0049")
non_digits_re = re.compile(r"\D")
fixme_re = re.compile(r'fixme', re.IGNORECASE)
lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\&<>;\'"\?%#$@\,\.\t\r\n]')

fixme_list =[]
CREATED = [ "version", "changeset", "timestamp", "user", "uid"]


def audit_street(street_name):
    m = problemchars_streetname.search(street_name)
    if m:
        print u"Problem with the street name: {}".format(street_name)
    return street_name    

def audit_postcode(postcode): 
    postcode = int(postcode)
    if postcode > 41564 or postcode < 40210:
        print u"Problem with the postcode: {}".format(postcode)   
    return postcode

def audit_is_wheelchair(is_wheelchair):
    mapping = {"Yes":"yes", "No":"no", "Limited": "limited"}
    if set([is_wheelchair]) < set(["Yes", "No", "Limited"]):
        return mapping[is_wheelchair]
    else:
        return is_wheelchair

def audit_phone_number(phone): 
    # remove hyphons
    phone = "".join(phone.split("-")[:])
    # remove white spaces
    phone = "".join(phone.split()[:])
    # remove slashes
    phone = "".join(phone.split(r"/")[:])
    # check if the country code is there
    if phone_re.search(phone):
        # take only the first phone number if there are more than one
        m = problemchars_phone.search(phone)
        if m:
            char = m.group()
          #  print "Problem character: " + char
            return phone.split(char)[0]
        elif phone_re_0049.search(phone):
            return "+49"+ phone.strip("0049")
        else:
            return phone
    elif non_digits_re.search(phone):
        return None
    else:
        return "+49" + phone.strip("0")

def audit_fixme(fixme):
    fixme_list.append(fixme)


def print_fixme(fixme_list):
    print 'The number of key=fixme or key=FIXME is {}.'.format(len(fixme_list))
    print 'Here are 10 examples: \n'
    pprint.pprint(fixme_list[0:10])
    
    
def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way" :
        # get the list of keys of the "node"/"way" dict
        keys = element.attrib.keys()
        # define the sub-dictionary 
        dic_created = {}
        dic_address = {}
        dic_contact = {}
        dic_created.fromkeys(CREATED)
        _type = element.tag
        # check the existence of the keys
        if set(["id", "changeset","user","version","uid","timestamp"]) <= set(keys):
           # print element.attrib
            _id = element.attrib["id"]
            dic_created["changeset"] = element.attrib["changeset"]
            dic_created["user"] = element.attrib["user"]
            dic_created["version"] = element.attrib["version"]
            dic_created["uid"] = element.attrib["uid"]
            dic_created["timestamp"] = element.attrib["timestamp"]
            node = {"id":_id, "general_type": _type, "created": dic_created }
        if set(["lat", "lon"]) <= set(keys):
            lat = float(element.attrib["lat"])
            lon = float(element.attrib["lon"])
            node["pos"] = [lon,lat]
        # for ways: make a list of node refs
        if element.tag == "way":   
            node_refs = []
            for tag in element.iter("nd"):
               # print "Key: {}, Value: {}".format(tag.attrib["k"],tag.attrib["v"])  
               # print tag.attrib["ref"]
                node_refs.append(tag.attrib["ref"]) 
            node["node_refs"] = node_refs
        # iterate over the tags
        for tag in element.iter("tag"):
            # select tags with one colon  
            if lower_colon.search(tag.attrib["k"]):
                colon_list = tag.attrib["k"].split(":")
                # create an address dict 
                if colon_list[0] == "addr":
                    address_type = colon_list[1]
                    # audit and clean the street name if necessary
                    if address_type == "street":
                        address_value = audit_street(tag.attrib["v"])
                    # audit and clean the postcode if necessary    
                    elif address_type == "postcode":  
                        address_value = audit_postcode(tag.attrib["v"])
                    else:
                        address_value = tag.attrib["v"]
                    # ignore values with problematic characters
                    # if not is_problemchars(address_value):
                    dic_address[address_type] = address_value
                # create a contact dict
                elif colon_list[0] == "contact":
                    contact_type = colon_list[1]
                    # audit and clean the street name if necessary
                    if contact_type == "phone":
                        contact_value = audit_phone_number(tag.attrib["v"])
                    # audit and clean the postcode if necessary    
                    elif contact_type == "fax":  
                        contact_value = audit_phone_number(tag.attrib["v"])
                    elif contact_type == "website":  
                        contact_value = tag.attrib["v"]
                    elif contact_type == "email":  
                        contact_value = tag.attrib["v"]    
                    else:
                        contact_value = tag.attrib["v"]
                    # ignore values with problematic characters
                    # if not is_problemchars(address_value):
                    dic_contact[contact_type] = contact_value
                # other cases with colon but without "addr"
                else:                   
                    value = tag.attrib["v"]
                    # ignore values with problematic characters
                    if not is_problemchars(value):
                        s = " "
                        key_string = s.join(colon_list)   
                        node[key_string] = value                    
            # select tags with lower case 
            if lower.search(tag.attrib["k"]): 
                key = tag.attrib["k"]
                value = tag.attrib["v"]
                # ignore values with problematic characters
                if not is_problemchars(value):
                    if key == "phone":
                        # print key
                        node[key] = audit_phone_number(value)
                    elif key == "wheelchair":
                        node[key] = audit_is_wheelchair(value)
                    else:
                        node[key] = value  
            # print problematic characters
            if is_problemchars(tag.attrib["v"]):
               # print tag.attrib["v"]
                pass
        # insert the address and contact dict into the node dict
        if dic_address:
            node["address"] = dic_address
        if dic_contact:
            node["contact"] = dic_contact
       
        return node
    else:
        return None

def is_problemchars(string):
    return bool(problemchars.search(string))

def process_map(file_in, pretty = False):
    # You do not need to change this file
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
    return data

def test():
    # NOTE: if you are running this code on your computer, with a larger dataset, 
    # call the process_map procedure with pretty=False. The pretty=True option adds 
    # additional spaces to the output, making it significantly larger.
    data = process_map(OSMFILE)
    print "Number of dictionaries: {}".format(len(data))
    pprint.pprint(data[0:9])
    
if __name__ == "__main__":
    test()