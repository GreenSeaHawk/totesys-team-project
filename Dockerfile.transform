FROM public.ecr.aws/lambda/python:3.13
COPY lambda/transform/src /var/task
RUN pip install pandas pyarrow iso4217
CMD ["transform_handler.transform_handler"]