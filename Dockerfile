FROM python:3.9
WORKDIR ./
COPY ./requirement.txt ./requirement.txt
RUN pip install -r requirement.txt
COPY ./http_post.py ./http_post.py
CMD ["python3","./http_post.py"]
