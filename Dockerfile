FROM python:3

ADD setup.py /

RUN pip install setuptools

CMD [ "python", "./setup.py" ]
