FROM python:3

WORKDIR /connector

COPY . .

EXPOSE 3000

CMD ["python", "./main.py"]

# Run from terminal by navigating to CoolVines and entering:
# GIT_USER=<your username> GIT_PASS=<your password> docker build cvpcm-web