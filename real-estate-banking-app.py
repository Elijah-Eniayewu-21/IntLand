from datetime import datetime
from decimal import Decimal
from typing import List, Optional
from pydantic import BaseModel
import uuid
import sqlite3
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import OAuth2PasswordBearer
from passlib.context import CryptContext

# Data Models
class User(BaseModel):
    id: str
    username: str
    email: str
    full_name: str
    role: str
    created_at: datetime

class Property(BaseModel):
    id: str
    title: str
    address: str
    country: str
    price: Decimal
    currency: str
    status: str
    owner_id: Optional[str]
    created_at: datetime

class Transaction(BaseModel):
    id: str
    property_id: str
    buyer_id: str
    seller_id: str
    amount: Decimal
    currency: str
    status: str
    created_at: datetime

# Database Connection
def get_db():
    conn = sqlite3.connect('real_estate_banking.db')
    try:
        yield conn
    finally:
        conn.close()

# Initialize FastAPI app
app = FastAPI(title="International Real Estate Banking System")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Database initialization
def init_db():
    conn = next(get_db())
    cursor = conn.cursor()
    
    # Create Users table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        username TEXT UNIQUE,
        email TEXT UNIQUE,
        full_name TEXT,
        role TEXT,
        password_hash TEXT,
        created_at TIMESTAMP
    )
    ''')
    
    # Create Properties table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS properties (
        id TEXT PRIMARY KEY,
        title TEXT,
        address TEXT,
        country TEXT,
        price DECIMAL,
        currency TEXT,
        status TEXT,
        owner_id TEXT,
        created_at TIMESTAMP,
        FOREIGN KEY (owner_id) REFERENCES users (id)
    )
    ''')
    
    # Create Transactions table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        id TEXT PRIMARY KEY,
        property_id TEXT,
        buyer_id TEXT,
        seller_id TEXT,
        amount DECIMAL,
        currency TEXT,
        status TEXT,
        created_at TIMESTAMP,
        FOREIGN KEY (property_id) REFERENCES properties (id),
        FOREIGN KEY (buyer_id) REFERENCES users (id),
        FOREIGN KEY (seller_id) REFERENCES users (id)
    )
    ''')
    
    conn.commit()

# Property Management
class PropertyService:
    @staticmethod
    async def list_properties(
        db: sqlite3.Connection,
        country: Optional[str] = None,
        max_price: Optional[float] = None
    ) -> List[Property]:
        cursor = db.cursor()
        query = "SELECT * FROM properties WHERE 1=1"
        params = []
        
        if country:
            query += " AND country = ?"
            params.append(country)
        
        if max_price:
            query += " AND price <= ?"
            params.append(max_price)
        
        cursor.execute(query, params)
        return [Property(**dict(zip(['id', 'title', 'address', 'country', 'price', 'currency', 'status', 'owner_id', 'created_at'], row))) 
                for row in cursor.fetchall()]

    @staticmethod
    async def create_property(
        db: sqlite3.Connection,
        property_data: dict
    ) -> Property:
        property_id = str(uuid.uuid4())
        cursor = db.cursor()
        
        cursor.execute('''
        INSERT INTO properties (id, title, address, country, price, currency, status, owner_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            property_id,
            property_data['title'],
            property_data['address'],
            property_data['country'],
            property_data['price'],
            property_data['currency'],
            'available',
            property_data.get('owner_id'),
            datetime.utcnow()
        ))
        
        db.commit()
        return Property(id=property_id, **property_data)

# Transaction Processing
class TransactionService:
    @staticmethod
    async def create_transaction(
        db: sqlite3.Connection,
        property_id: str,
        buyer_id: str,
        amount: Decimal,
        currency: str
    ) -> Transaction:
        # Get property details
        cursor = db.cursor()
        cursor.execute("SELECT owner_id, status FROM properties WHERE id = ?", (property_id,))
        property_data = cursor.fetchone()
        
        if not property_data:
            raise HTTPException(status_code=404, detail="Property not found")
        
        owner_id, status = property_data
        
        if status != 'available':
            raise HTTPException(status_code=400, detail="Property is not available for purchase")
        
        # Create transaction
        transaction_id = str(uuid.uuid4())
        cursor.execute('''
        INSERT INTO transactions (id, property_id, buyer_id, seller_id, amount, currency, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            transaction_id,
            property_id,
            buyer_id,
            owner_id,
            amount,
            currency,
            'pending',
            datetime.utcnow()
        ))
        
        # Update property status
        cursor.execute('''
        UPDATE properties
        SET status = 'under_contract'
        WHERE id = ?
        ''', (property_id,))
        
        db.commit()
        
        return Transaction(
            id=transaction_id,
            property_id=property_id,
            buyer_id=buyer_id,
            seller_id=owner_id,
            amount=amount,
            currency=currency,
            status='pending',
            created_at=datetime.utcnow()
        )

# API Routes
@app.post("/properties/")
async def create_property(
    property_data: dict,
    db: sqlite3.Connection = Depends(get_db)
):
    return await PropertyService.create_property(db, property_data)

@app.get("/properties/")
async def list_properties(
    country: Optional[str] = None,
    max_price: Optional[float] = None,
    db: sqlite3.Connection = Depends(get_db)
):
    return await PropertyService.list_properties(db, country, max_price)

@app.post("/transactions/")
async def create_transaction(
    property_id: str,
    buyer_id: str,
    amount: float,
    currency: str,
    db: sqlite3.Connection = Depends(get_db)
):
    return await TransactionService.create_transaction(
        db,
        property_id,
        buyer_id,
        Decimal(str(amount)),
        currency
    )

# Initialize database on startup
@app.on_event("startup")
async def startup_event():
    init_db()
