FROM python:3-alpine
RUN apk update
RUN pip3 install --upgrade pip
RUN apk --update add build-base libffi-dev libressl-dev python-dev py-pip
RUN pip install cryptography
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY ./service /service
EXPOSE 5000
CMD ["python3","-u","./service/datasource-service.py"]