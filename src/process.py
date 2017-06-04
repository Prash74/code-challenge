#!/usr/bin/python
import pandas as pd
import json
import datetime
import os
from tabulate import tabulate
pd.set_option('display.width', 200)


class Shutterfly:
    """
    To analyze acquisition strategy and estimate marketing cost through the
    calculation the Lifetime Value ('LTV') of a customer

    Attributes: (Pandas DataFrame)
        customers  : To store all customer personal details
        orders     : To store order details of every customer
        images     : To store details of each image uploaded by every customer
        site_visits: To store site visit information of every customer
        ltv        : To store the Lifetime Value during analysis
        size       : Variable to hold current Datastore Size
    """
    def __init__(self):
        """Initializing the data structore to store all events"""
        self.customers = pd.DataFrame(columns=['key', 'event_time',
                                               'last_name', 'adr_city',
                                               'adr_state'])
        self.orders = pd.DataFrame(columns=['key', 'event_time', 'customer_id',
                                            'total_amount'])
        self.images = pd.DataFrame(columns=['key', 'event_time', 'customer_id',
                                            'camera_make', 'camera_model'])
        self.site_visits = pd.DataFrame(columns=['key', 'event_time',
                                                 'customer_id', 'tags'])
        self.ltv = pd.DataFrame(columns=['Customer ID', 'Customer Last Name',
                                         'Total Revenue', 'Number of Visits',
                                         'LTV value'])
        self.size = 0

    def update_size(self):
        """To update the Datastore size after each ingestion"""
        self.size = len(self.customers) + len(self.orders) + len(self.images)
        self.size += len(self.site_visits)

    def get_size(self):
        """To Obtain current Datastore Size"""
        return self.size

    def Ingest(self, e):
        """To process incoming events and update the Master Data"""
        new, update, dq, dup = 0, 0, 0, 0
        dq_records = []
        for ev in e:
            """To run Data Quality Check on each event being processed"""
            chk = self.DQ_check(ev)
            if chk:
                if ev["type"] == "CUSTOMER":
                    if ev["verb"] == "NEW":
                        if ev["key"] in self.customers["key"].values:
                            dup += 1
                            continue
                        new += 1
                        index = len(self.customers)
                        for key in ev.keys():
                            if key != "type" and key != "verb":
                                self.customers.loc[index, key] = ev[key]
                    elif ev["verb"] == "UPDATE":
                        update += 1
                        for key in ev.keys():
                            if key != "type" and key != "verb":
                                self.customers.loc[self.customers["key"] ==
                                                   ev["key"], key] = ev[key]
                elif ev["type"] == "SITE_VISIT":
                    if ev["key"] in self.site_visits["key"].values:
                        dup += 1
                        continue
                    new += 1
                    index = len(self.site_visits)
                    for key in ev.keys():
                        if key != "type" and key != "verb" and key != "tags":
                            self.site_visits.loc[index, key] = ev[key]
                elif ev["type"] == "IMAGE":
                    if ev["key"] in self.images["key"].values:
                        dup += 1
                        continue
                    new += 1
                    index = len(self.images)
                    for key in ev.keys():
                        if key != "type" and key != "verb":
                            self.images.loc[index, key] = ev[key]
                elif ev["type"] == "ORDER":
                    if ev["verb"] == "NEW":
                        if ev["key"] in self.orders["key"].values:
                            dup += 1
                            continue
                        new += 1
                        index = len(self.orders)
                        for key in ev.keys():
                            if key != "type" and key != "verb":
                                if key == "total_amount":
                                    self.orders.loc[index, key]\
                                        = float(ev[key][:-4])
                                else:
                                    self.orders.loc[index, key] = ev[key]
                    elif ev["verb"] == "UPDATE":
                        update += 1
                        for key in ev.keys():
                            if key != "type" and key != "verb":
                                if key == "total_amount":
                                    self.orders.loc[self.orders["key"] ==
                                                    ev["key"], key] \
                                                = float(ev[key][:-4])
                                else:
                                    self.orders.loc[self.orders["key"] ==
                                                    ev["key"], key] = ev[key]
            else:
                dq_records.append([ev, "Invalid Record due to missing key"])
                dq += 1

        """To check if any records are rejected due to DQ errors"""
        if len(dq_records) > 0:
            self.write_dq(dq_records)

        print "================================================"
        print "Current Data Store Size : %d" % self.get_size()
        print "================================================"
        print "Number of Events Ingested           : %d" % new
        print "Number of Events Updated            : %d" % update
        print "Number of Events Rejected due to DQ : %d" % dq
        print "Number of Duplicat Events           : %d" % dup
        print "================================================"
        self.update_size()
        print "Updated Data Store Size : %d" % self.get_size()
        print "Ingestion Completed at : ", datetime.datetime.now().time()
        print "================================================"

        """To write the log for the current Ingestion"""
        self.log(new, update, dq, dup)

    def TopXSimpleLTVCustomers(self, x):
        """To display Top X customers based on their Lifetime Value ('LTV')"""
        self.ltv = pd.DataFrame(columns=['key', 'Customer Last Name',
                                         'LTV value'])
        print "\nDisplaying Top %s Customers based on LTV Value as of %s"\
              % (x, datetime.datetime.now())
        revenue = self.orders[['customer_id',
                               'total_amount']].groupby('customer_id').sum()
        visits = self.site_visits['customer_id'].value_counts()
        customers = list(set(self.customers['key']))
        for i in customers:
            try:
                idx = len(self.ltv)
                weeks = self.site_visits.loc[self.site_visits['customer_id'] == i, 'event_time'].tolist()
                if len(weeks) == 1:
                    num_wks = 1
                elif len(weeks) == 0:
                    continue
                elif len(weeks) > 1:
                    num_wks = (self.calc_julian(max(weeks)) - self.calc_julian(min(weeks))) / 7
                    num_wks = int(num_wks)
                    if num_wks == 0:
                        num_wks = 1
                a = (revenue.loc[i, 'total_amount'] / visits.loc[i]) * \
                    (visits.loc[i] / num_wks)
                ltv = 52 * a * 10
                self.ltv.loc[idx, 'key'] = idx+1
                self.ltv.loc[idx, 'Customer ID'] = i
                self.ltv.loc[idx, 'Customer Last Name'] = self.customers.loc[self.customers['key'][self.customers['key'] == i].index[0], 'last_name']
                self.ltv.loc[idx, 'Total Revenue'] = revenue.loc[i, 'total_amount']
                self.ltv.loc[idx, 'Number of Visits'] = visits.loc[i]
                self.ltv.loc[idx, 'LTV value'] = int(ltv)
            except:
                pass

        self.ltv.sort_values(by="LTV value", inplace=True, ascending=False)
        print tabulate(self.ltv.head(int(x)), headers='keys', tablefmt='psql')

    def calc_julian(self, n):
        dt = n[:10].split("-")
        dt = [int(i) for i in dt]
        dt2 = datetime.date(dt[0], dt[1], dt[2])
        return dt2.toordinal() + 1721424.5

    def log(self, new, update, dq, dup):
        """To create log file to store current ingestion details"""
        logfile_nm = str(datetime.datetime.now().time()) + "_log.txt"
        fl = open(os.path.join("logs", logfile_nm), "w+")
        fl.write("================================================")
        fl.write("\nCurrent Data Store Size : " + str(self.get_size()))
        fl.write("\n================================================")
        fl.write("\nNumber of Events Ingested           : " + str(new))
        fl.write("\nNumber of Events Updated            : " + str(update))
        fl.write("\nNumber of Events Rejected due to DQ : " + str(dq))
        fl.write("\nNumber of Duplicat Events           : " + str(dup))
        fl.write("\n================================================")
        fl.write("\nUpdated Data Store Size : " + str(self.get_size()))
        fl.write("\nIngestion Completed at : " + str(datetime.datetime.now().time()))
        fl.write("\n================================================")
        fl.close()

    def DQ_check(self, ev):
        """To run DQ checks on each incoming Event"""
        if ev["type"] == "CUSTOMER":
            if ev["verb"] == "UPDATE":
                if ev["key"] not in self.customers["key"].values:
                    return False
                else:
                    return True
            else:
                return True
        elif ev["type"] == "ORDER":
            if ev["verb"] == "UPDATE":
                if ev["key"] not in self.orders["key"].values:
                    return False
                else:
                    return True
            else:
                return True
        else:
            return True

    def display(self):
        """To display the Datastore in Table Format"""
        print "Customers Table"
        print tabulate(self.customers, headers='keys', tablefmt='psql')
        print "Site Visit Table"
        print tabulate(self.site_visits, headers='keys', tablefmt='psql')
        print "Images Table"
        print tabulate(self.images, headers='keys', tablefmt='psql')
        print "Orders Table - total_amount in USD"
        print tabulate(self.orders, headers='keys', tablefmt='psql')

    def write_dq(self, dq_records):
        """To write rejected events to a DQ Log File"""
        dqfile_nm = str(datetime.datetime.now().time()) + "_dq_log.txt"
        fil = open(os.path.join("logs", dqfile_nm), "w+")
        for i in dq_records:
            fil.write(str(i)+"\n")
        fil.close()


def main():
    sh = Shutterfly()
    while(True):
        print "\nEnter your option:"
        print "1.Ingest 2.LTV 3.Display 4.Exit"
        opt = raw_input("Option: ")
        if opt == 'Ingest' or opt == "1":
            # "../sample_input/events.txt"
            file_nm = raw_input("Enter File Name with Path : ")
            with open(file_nm) as f:
                data = json.load(f)
            sh.Ingest(data)
        elif opt == 'LTV' or opt == "2":
            x = raw_input("Enter the Value for x: ")
            sh.TopXSimpleLTVCustomers(x)
        elif opt == 'Display' or opt == "3":
            sh.display()
        elif opt == 'Exit' or opt == "4":
            print "Exiting......"
            break
        else:
            print "Please enter the correct option"

if __name__ == '__main__':
    main()
