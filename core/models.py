from pydantic import BaseModel
from typing import Optional


class Company(BaseModel):
    name: str
    website: Optional[str] = ""
    linkedin_company: Optional[str] = ""
    industry: Optional[str] = ""
    company_size: Optional[str] = ""
    location: Optional[str] = ""
    description: Optional[str] = ""


class Lead(Company):
    scraped_at: Optional[str] = ""
    emails: Optional[str] = ""
    phones: Optional[str] = ""
    company_keywords: Optional[str] = ""
    pain_points: Optional[str] = ""
    status: Optional[str] = "Extracted"
    email_sent_at: Optional[str] = ""

    def to_sheet_row(self) -> list:
        """Serialize to a flat list matching the Outreach Sheet column order."""
        return [
            self.name, self.website, self.linkedin_company, self.industry,
            self.company_size, self.location, self.description, self.scraped_at,
            self.emails, self.phones, self.company_keywords, self.pain_points,
            self.status, self.email_sent_at,
        ]
