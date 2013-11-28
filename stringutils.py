#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unicodedata
from unidecode import unidecode
from string import punctuation



# http://www.tablix.org/~avian/blog/archives/2009/01/unicode_transliteration_in_python/


def suchString(inp):
    """ aus nicht-ascii-chars syscomp-suchkonforme strings machen 
    """
    # zuerst replace von Umlauten mit Umschrift, bzw ss 
    # dann unicodedata.normalize
    # dann upper
    key = inp
    key = key.replace(u"ä", u"ae")
    key = key.replace(u"ö", u"oe")
    key = key.replace(u"ü", u"ue")
    key = key.replace(u"Ä", u"Ae")
    key = key.replace(u"Ö", u"Oe")
    key = key.replace(u"Ü", u"Ue")
    key = key.replace(u"ß", u"ss")

    #key = key.translate(None, ''' -_!"'§$%&/()=''')
    #out = key.translate(' .,;#+*´`}{[]-_!"§$%&/()=')
    # not possible even if i encode to ascii, i cannot translate 
    # sth to '' nothing, which is what is wanted.

    #[ c.replace(a, '') for a in key ]
    new = ''
    for c in key:
        if c in punctuation:
            new += ''
        else:
            new += c

    new = new.replace(' ','')
    #norm= unicodedata.normalize('NFKD', key).encode('ascii','ignore')
    upp = unidecode(new)
    upp = upp.upper()

    return upp
    




def din5007(input):
    """ This function implements sort keys for the german language according to 
    DIN 5007."""
    
    # key1: compare words lowercase and replace umlauts according to DIN 5007
    key1=input.lower()
    key1=key1.replace(u"ä", u"a")
    key1=key1.replace(u"ö", u"o")
    key1=key1.replace(u"ü", u"u")
    key1=key1.replace(u"ß", u"ss")
    
    # key2: sort the lowercase word before the uppercase word and sort
    # the word with umlaut after the word without umlaut
    key2=input.swapcase()
    
    # in case two words are the same according to key1, sort the words
    # according to key2. 
    return (key1, key2)
    
