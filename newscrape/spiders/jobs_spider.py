import scrapy
import lxml
from unidecode import unidecode

#0
class PostSpider(scrapy.Spider):
    name = "jobs"
    def clean_html(self, content):
        if content and len(content) > 0:
            tagless = lxml.html.fromstring(content).text_content()
            return unidecode(tagless).strip()
        return None
    #1
    start_urls = [
        "https://jobs.toronto.ca/jobsatcity/tile-search-results/?q=&sortColumn=referencedate&sortDirection=desc",
        "https://jobs.toronto.ca/jobsatcity/tile-search-results/?q=&sortColumn=referencedate&sortDirection=desc&startrow=25",
        "https://jobs.toronto.ca/jobsatcity/tile-search-results/?q=&sortColumn=referencedate&sortDirection=desc&startrow=50",
        "https://jobs.toronto.ca/jobsatcity/tile-search-results/?q=&sortColumn=referencedate&sortDirection=desc&startrow=75"
    ]
    #2
    scraped_jobs = set() # holds all job titles to not scrape duplicates

    def parse(self, response):
        jobLinks = response.css('a.jobTitle-link::attr(href)').getall()
        for jobLink in jobLinks:
            if jobLink in self.scraped_jobs: continue
            self.scraped_jobs.add(jobLink)
            newUrl = response.urljoin("https://jobs.toronto.ca"+jobLink)
            yield scrapy.Request(newUrl, callback=self.parsePage)
    #8
    def isDescriptionValid(self, description):
        if len(description) > 100 or "responsibilities" in description or "responsibilites" in description or ': ' not in description:
            return False
        return True

    #7
    def getJobDescriptions(self, allHtml):
        ret = {}
        idx = allHtml.find("job id:")
        if (idx == -1): idx = allHtml.find("position id")
        if (idx == -1): return ret, ""
        raw = allHtml[idx:idx+1000].split("\r\n")
        summary = ""
        for i in range(len(raw)):
            description = raw[i].strip()
            if (description == ""): continue
            if not self.isDescriptionValid(description):
                if ("job summary" in description):
                    for j in range(i+1, len(raw)):
                        if (raw[j].strip() != ""):
                            summary = raw[j]
                            break
                break
            seperated = description.split(': ')
            ret[seperated[0]] = seperated[1]
        return ret, summary
    #3
    def parsePage(self, response):
        job = {"Link": response.url}
        job_title = self.clean_html(response.xpath('/html/body/div[2]/div[2]/div/div/div[2]/div/div[1]/div[2]/div[1]/div/div/div/h1/span').get()).strip()
        allHtml = self.clean_html(response.xpath("/html/body").get()).lower()
        #4
        responsibilityList = ["responsibilities", "responsibilites", "what will you do?"] # "responsibilites" is needed because some jobs don't even spell responsibilities correctly
        qualificationList = ["qualifications", "what do you bring to the role"]
        also_haves = ["you must also have", "must also have", "also have"]
        #5
        for responsibilityWord in responsibilityList:
            responsibility_index = allHtml.find(responsibilityWord)
            if responsibility_index != -1: break

        for qualificationWord in qualificationList:
            qualification_index = allHtml.find(qualificationWord)
            if qualification_index != -1: break

        for haveWord in also_haves:
            alsoHave_index = allHtml.find(haveWord);
            if (alsoHave_index != -1): break

        end_index = -1
        end_index_search = ["we thank all applicants and advise that only those selected for further consideration will be contacted", "on-the-job training program", "note to current city of toronto employees","please note", "note: as a condition of employment with the lon", "yoronto is home to more than", "how to apply", "note to internal full", "equity, diversity and inclusion", "accommodation"]
        for index in end_index_search:
            curIndex = allHtml.find(index)
            if (curIndex != -1):
                end_index = curIndex
                break

        alsoHave_raw = ""
        qualification_end_index = alsoHave_index-6 if alsoHave_index != -1 else end_index
        qualification_raw = list(map(lambda text : text.strip(), allHtml[qualification_index:qualification_end_index].strip().split("\n")))
        qualification_refined = list(filter(lambda y : y!="", qualification_raw))[1:]

        responsibility_raw = list(map(lambda text : text.strip(), allHtml[responsibility_index:(qualification_index if qualification_index != -1 else end_index)].strip().split("\n")))
        major_responsibilites_refined = list(filter(lambda y : y!="", responsibility_raw))[1:]
        
        #6
        jobDescriptions, summary = self.getJobDescriptions(allHtml)
        
        #9
        if (len(major_responsibilites_refined) == 0): major_responsibilites_refined = "does not exist"
        if (len(jobDescriptions) == 0): jobDescriptions = "does not exist"

        job["Job Description"] = jobDescriptions
        if (summary != ""): job["Job Summary"] = summary
        job["Major Responsibilities"] = major_responsibilites_refined
        job["Qualifications"] = qualification_refined

        if (alsoHave_index != -1):
            alsoHave_raw = list(map(lambda text : text.strip(), allHtml[alsoHave_index:end_index].strip().split("\n")))
            alsoHave_refined = list(filter(lambda y : y!="", alsoHave_raw))[1:]
            job["You Must Also Have"] = alsoHave_refined

        yield {
            job_title: job
        }
# scrapy crawl jobs -o jobs.json