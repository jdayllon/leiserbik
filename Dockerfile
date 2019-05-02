FROM jfloff/alpine-python:3.7-slim
#FROM frankierr/docker-apertium-all-dev
MAINTAINER Juan David Ayllón Burguillo <jdayllon@gmail.com>

USER root

# Install pipenv
RUN pip install --upgrade pip
RUN pip install pipenv

# Install Leiserbik
WORKDIR /tmp
RUN wget -P /tmp https://github.com/jdayllon/leiserbik/archive/master.zip
RUN unzip master.zip
WORKDIR /tmp/leiserbik-master
RUN ls -al
RUN python setup.py install

#COPY requirements.txt apertium-apy/
#RUN pip install --upgrade pip
#RUN pip3 install -r /apertium-apy/requirements.txt
#RUN pip3 install tornado

#COPY . apertium-apy
#RUN cd apertium-apy && make -j2
#RUN ls

# Run APy
#RUN sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
#    locale-gen
#ENV LANG en_US.UTF-8
#ENV LANGUAGE en_US:en
#ENV LC_ALL en_US.UTF-8

#EXPOSE 2737
#ENTRYPOINT ["python3.5", "/apertium-apy/servlet.py", "--lang-names", "/apertium-apy/langNames.db"]
#CMD ["/usr/share/apertium/modes", "--port", "2737"]
