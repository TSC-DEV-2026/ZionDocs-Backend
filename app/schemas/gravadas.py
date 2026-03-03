from datetime import date, time
from pydantic import BaseModel

class Team(BaseModel):
    id: int
    code: str
    name: str

class Employee(BaseModel):
    id: int
    name: str
    nis: str
    email: str
    cpf: str
    registration_number: str
    team: Team

class GravadasCreate(BaseModel):
    date: date
    time: time
    address: str
    employee: Employee

class Gravadas(BaseModel):
    id: int
    registration_number: str
    date: date
    time: time

class Config:
    from_attributes = True