from pydantic import BaseModel, EmailStr

class RegisterUserCommand(BaseModel):
    username: str
    email: str
    password: str
