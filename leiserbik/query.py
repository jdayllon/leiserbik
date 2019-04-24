from leiserbik import *
import operator

class TwitterQuery:

    def __init__(self):
        self._screen_name = None
        self._screen_name_from = True
        self._screen_name_to = None
        self._screen_name_on = None
        self._geolocation = None
        self._location = []
        self._hashtags = []
        self._start_date = None
        self._end_date = None
        self._phrase = None
        self._words = []
        self._operator = operator.or_


    @property
    def operator(self):
        return self._operator

    @operator.setter
    def operator(self, type_operator):
        assert type_operator == operator.or_ or type_operator == operator.and_
        self._operator = type_operator

    @property
    def screen_name(self):
        return self._screen_name

    @screen_name.setter
    def screen_name(self, screen_name, screen_name_from = True, screen_name_to = False, screen_name_on = False ):
        self._screen_name = screen_name.replace("#","")

        self._screen_name_from = screen_name_from
        self._screen_name_to = screen_name_to
        self._screen_name_on = screen_name_on


    @property
    def geolocation(self):
        return self._geolocation

    @geolocation.setter
    def geolocation(self, geolocation):
        self._geolocation = geolocation

    @property
    def location(self):
        return self._location

    @geolocation.setter
    def location(self, location):
        self._location = location

    @property
    def hashtags(self):
        return self._hashtags

    @hashtags.setter
    def hashtags(self,hashtags):
        self._hashtags = hashtags

    @property
    def start_date(self):
        return self._start_date.format(SHORT_DATE_FORMAT)

    @start_date.setter
    def start_date(self, date):
        if type(date) is str:
            self._start_date = arrow.get(date, SHORT_DATE_FORMAT)


    @property
    def end_date(self):
        return self._end_date.format(SHORT_DATE_FORMAT)


    @property.setter
    def end_date(self,date ):
        if type(date) is str:
            self._end_date = arrow.get(date, SHORT_DATE_FORMAT)

    @property
    def phrase(self):
        return self._phrase

    @phrase.setter
    def phrase(self, phrase):
        self._phrase = phrase

    @property
    def words(self):
        return self._words

    @words.setter
    def words(self,words):
        self._words = words


    def query(self):
        query_terms = []

        if self._screen_name is not None:
            if self._screen_name_from:
                query_terms += [f"from:{self._screen_name}"]
            if self._screen_name_to:
                query_terms += [f"to:{self._screen_name}"]
            if self._screen_name_on:
                query_terms += [f"on:{self._screen_name}"]

        if self._geolocation is not None:
            # geocode:latitude,longitude,radius
            # geocode:46.189770,-123.833946,10km
            query_terms += [f"geocode:{self._geolocation}"]

        if len(self._location) > 0:
            #near:"Gerena, España" within:15mi&src=typd

            location_refs = ",".join(self._location)

            query_terms += [f"near:{location_refs}"]

        if type(self._hashtags) is list and len(self._hashtags) > 0:

            query_terms += [" OR ".join(self._hashtags)]

        elif type(self._hashtags) is str and len(self._hashtags) > 0:

            query_terms += [self._hashtags]

        if self._start_date is not None:
            query_terms += [f"since:{self._start_date}"]

        if self._end_date is not None:
            query_terms += [f"until:{self._end_date}"]

        if self._phrase is not None:
            query_terms += [f""" "{self._phrase}" """]

        if type(self._words) is list and len(self._words) > 0:

            query_terms += [" OR ".join(self._words)]

        elif type(self._words) is str and len(self._words) > 0:

            query_terms += [self._words]

        if self._operator == operator.or_:

            query_string = " OR ".join(query_terms)

        else:

            query_string = " AND ".join(query_terms)

        return query_string

    def __str__(self):
        return self.query(self)
