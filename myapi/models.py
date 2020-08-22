from django.db import models
from django.core.validators import FileExtensionValidator
import os

class FileModel(models.Model):
    keys = models.CharField(max_length=100)
    input_id = models.IntegerField()
    file = models.FileField(validators=[FileExtensionValidator(allowed_extensions=['xlsx','xlsb'])])
    uploaded_date = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return os.path.basename(self.file.name)
