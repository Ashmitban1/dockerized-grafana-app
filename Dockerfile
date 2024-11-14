FROM python:3.11.9

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt

RUN pip install --upgrade pip
RUN pip install -r /code/requirements.txt

COPY ./app.py /code
COPY . .

EXPOSE 5000  

CMD ["python", "app.py"]
