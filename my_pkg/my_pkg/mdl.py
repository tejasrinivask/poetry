from pydantic import BaseModel, Field
from typing import List

class MyModel(BaseModel):
    items: List[int] = Field(default_factory=list)

def mdl():
    model = MyModel()
    # print(model.items)  # Output: []
    return model.items