from pydantic import BaseModel

# Define a Pydantic model for message data validation and serialization
# This model specifies the expected structure and data types for messages, ensuring that each message
# adheres to this structure. The use of strings for all fields, including timestamp, provides flexibility,
# but consider using more specific types (e.g., datetime for 'timestamp') for stricter validation.
class MessageModel(BaseModel):
    businessUnitId: str
    reconciliationId: str
    reconciliationType: str
    workstationId: str
    operatorId: str
    timestamp: str  # Consider using datetime for more specific type handling
    userId: str
    tenders: list
    transactionTenderDates: list
