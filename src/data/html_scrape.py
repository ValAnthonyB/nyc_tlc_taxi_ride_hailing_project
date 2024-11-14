# html_scraper

from bs4 import BeautifulSoup
import requests


def check_same_state(s3_client, s3_bucket_name):
    url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    response = requests.get(url)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract the years
        year = int(soup.find('div', class_="faq-questions").find("p").text)
    
        # Get the first table
        table = soup.find('table')

        # Get the month from the table
        month = table.find_all('strong')[-1].text

    # Current month and year
    curr_month_year = f"{month} {year}"

    # Get the previous state from S3
    s3_response = s3_client.get_object(Bucket=s3_bucket_name, Key="state/prev_state.txt")
    prev_month_year = s3_response['Body'].read().decode('utf-8')
    
    # Changes state if the curr and prev are not the same
    state = (curr_month_year == prev_month_year)

    return (state, soup, month, year)


def update_state_s3(s3_client, s3_bucket_name, month, year):
    """Updates the state file in S3."""
    try:
        s3_client.put_object(Bucket=s3_bucket_name, Key="state/prev_state.txt", Body=f"{month} {year}")

    except Exception as e:
        raise RuntimeError(f"Error updating the state file in S3: {str(e)}")



def link_extraction(soup):
    """Extracts all the links from the soup HTML"""

    try:
        # Extract the years
        div_faq_questions = soup.find_all('div', class_="faq-questions")
        years = [div.find("p").text for div in div_faq_questions]
    
        # Gets all the tables
        tables = soup.find_all('table')
    
        # Get the full dataset
        full_data_dict = {}
        for year, table in zip(years, tables):
            year_only_dict = {}
    
            # Iterates through the tds
            for td in table.find_all('td'):
                month = None
                # Extract the month and the links
                for element in td.find_all(['strong', 'ul']):
                    if element.name == 'strong':
                        month = element.get_text(strip=True)
                        year_only_dict[month] = {}
            
                    elif element.name == 'ul' and month:
                        for li in element.find_all('li'):
                            link = li.find('a')
                            title = link.get_text(strip=True)
                            url = link['href'].strip()
                            year_only_dict[month][title] = url
    
            # Year = Key, Months = Value
            full_data_dict[int(year)] = year_only_dict
    
        return full_data_dict

    except Exception as e:
        raise RuntimeError(f"Error in extracting links: {str(e)}")



