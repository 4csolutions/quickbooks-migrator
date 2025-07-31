# Copyright (c) 2018, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt


import json
import traceback
import re
import math
import frappe
import requests
from frappe import _
from frappe.utils import flt
from frappe.model.document import Document
from requests_oauthlib import OAuth2Session

from erpnext import encode_company_abbr, get_company_currency

# QuickBooks requires a redirect URL, User will be redirect to this URL
# This will be a GET request
# Request parameters will have two parameters `code` and `realmId`
# `code` is required to acquire refresh_token and access_token
# `realmId` is the QuickBooks Company ID. It is Needed to actually fetch data.
@frappe.whitelist()
def callback(*args, **kwargs):
	migrator = frappe.get_doc("QuickBooks Migrator")
	migrator.set_indicator("Connecting to QuickBooks")
	migrator.code = kwargs.get("code")
	migrator.quickbooks_company_id = kwargs.get("realmId")
	migrator.save()
	migrator.get_tokens()
	frappe.db.commit()
	migrator.set_indicator("Connected to QuickBooks")
	# We need this page to automatically close afterwards
	frappe.respond_as_web_page("Quickbooks Authentication", html="<script>window.close()</script>")


class QuickBooksMigrator(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		access_token: DF.SmallText | None
		api_endpoint: DF.Data
		authorization_endpoint: DF.Data
		authorization_url: DF.Data
		client_id: DF.Data
		client_secret: DF.Data
		code: DF.Data | None
		company: DF.Link | None
		default_cost_center: DF.Link | None
		default_shipping_account: DF.Link | None
		default_warehouse: DF.Link | None
		quickbooks_company_id: DF.Data | None
		redirect_url: DF.Data
		refresh_token: DF.SmallText | None
		scope: DF.Data
		status: DF.Literal[
			"Connecting to QuickBooks", "Connected to QuickBooks", "In Progress", "Complete", "Failed"
		]
		token_endpoint: DF.Data
		undeposited_funds_account: DF.Link | None
	# end: auto-generated types

	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.oauth = OAuth2Session(client_id=self.client_id, redirect_uri=self.redirect_url, scope=self.scope)
		if not self.authorization_url and self.authorization_endpoint:
			self.authorization_url = self.oauth.authorization_url(self.authorization_endpoint)[0]

	def on_update(self):
		if self.company:
			# We need a Cost Center corresponding to the selected erpnext Company
			self.default_cost_center = frappe.db.get_value("Company", self.company, "cost_center")
			company_warehouses = frappe.get_all("Warehouse", filters={"company": self.company, "is_group": 0})
			if company_warehouses:
				self.default_warehouse = company_warehouses[0].name
		if self.authorization_endpoint:
			self.authorization_url = self.oauth.authorization_url(self.authorization_endpoint)[0]

	@frappe.whitelist()
	def migrate(self):
		frappe.enqueue_doc("QuickBooks Migrator", "QuickBooks Migrator", "_migrate", queue="long")

	def _migrate(self):
		try:
			self.set_indicator("In Progress")
			# Add quickbooks_id field to every document so that we can lookup by Id reference
			# provided by documents in API responses.
			# Also add a company field to Customer Supplier and Item
			self._make_custom_fields()

			self._migrate_accounts()

			# Some Quickbooks Entities like Advance Payment, Payment aren't available firectly from API
			# Sales Invoice also sometimes needs to be saved as a Journal Entry
			# (When Item table is not present, This appens when Invoice is attached with a "StatementCharge" "ReimburseCharge
			# Details of both of these cannot be fetched from API)
			# Their GL entries need to be generated from GeneralLedger Report.
			self._fetch_general_ledger()

			# QuickBooks data can have transactions that do not fall in existing fiscal years in ERPNext
			self._create_fiscal_years()

			self._allow_fraction_in_unit()

			# Following entities are directly available from API
			# Invoice can be an exception sometimes though (as explained above).
			entities_for_normal_transform = [
				# "Customer",
				# "Item",
				# "Vendor",
				# "Preferences",
				# "JournalEntry",
				# "Purchase",
				# "Deposit",
				# "Invoice",
				# "CreditMemo",
				# "SalesReceipt",
				# "RefundReceipt",
				# "Bill",
				# "VendorCredit",
				# "Payment",
				# "BillPayment",
			]
			for entity in entities_for_normal_transform:
				self._migrate_entries(entity)

			# Following entries are not available directly from API, Need to be regenrated from GeneralLedger Report
			entities_for_gl_transform = [
				# "Journal Entry",
				# "Expense",
				# "Cheque Expense",
				# "Deposit", # Some Deposit entries with inclusive of taxes are properly handled in GL ex: txnid 12
				# "Payment",#Errors - 16
				# "Transfer",
				# "Refund",
				# "Invoice",
				# "Tax Credit Note",
				# "Bill",
				# "Bill Payment (Cheque)",#1 error exchange rate
				"Supplier Credit",
				"Advance Payment",
				"Tax Payment",
				"Sales Tax Payment",
				"Purchase Tax Payment",
				"Inventory Qty Adjust",
			]
			for entity in entities_for_gl_transform:
				self._migrate_entries_from_gl(entity)
			self.set_indicator("Complete")
		except Exception as e:
			self.set_indicator("Failed")
			self._log_error(e)

		frappe.db.commit()

	def get_tokens(self):
		token = self.oauth.fetch_token(
			token_url=self.token_endpoint, client_secret=self.client_secret, code=self.code
		)
		self.access_token = token["access_token"]
		self.refresh_token = token["refresh_token"]
		self.save()

	def _refresh_tokens(self):
		token = self.oauth.refresh_token(
			token_url=self.token_endpoint,
			client_id=self.client_id,
			refresh_token=self.refresh_token,
			client_secret=self.client_secret,
			code=self.code,
		)
		self.access_token = token["access_token"]
		self.refresh_token = token["refresh_token"]
		self.save()

	def _make_custom_fields(self):
		doctypes_for_quickbooks_id_field = [
			"Account",
			"Customer",
			"Address",
			"Item",
			"Supplier",
			"Sales Invoice",
			"Journal Entry",
			"Purchase Invoice",
		]
		for doctype in doctypes_for_quickbooks_id_field:
			self._make_custom_quickbooks_id_field(doctype)

		doctypes_for_company_field = ["Customer", "Item", "Supplier"]
		for doctype in doctypes_for_company_field:
			self._make_custom_company_field(doctype)

		frappe.db.commit()

	def _make_custom_quickbooks_id_field(self, doctype):
		if not frappe.get_meta(doctype).has_field("quickbooks_id"):
			frappe.get_doc(
				{
					"doctype": "Custom Field",
					"label": "QuickBooks ID",
					"dt": doctype,
					"fieldname": "quickbooks_id",
					"fieldtype": "Data",
				}
			).insert()

	def _make_custom_company_field(self, doctype):
		if not frappe.get_meta(doctype).has_field("company"):
			frappe.get_doc(
				{
					"doctype": "Custom Field",
					"label": "Company",
					"dt": doctype,
					"fieldname": "company",
					"fieldtype": "Link",
					"options": "Company",
				}
			).insert()

	def _migrate_accounts(self):
		self._make_root_accounts()
		for entity in ["Account", "TaxRate", "TaxCode"]:
			self._migrate_entries(entity)

	def _make_root_accounts(self):
		roots = ["Asset", "Equity", "Expense", "Liability", "Income"]
		for root in roots:
			try:
				if not frappe.db.exists(
					{
						"doctype": "Account",
						"name": encode_company_abbr(f"{root} - QB", self.company),
						"company": self.company,
					}
				):
					frappe.get_doc(
						{
							"doctype": "Account",
							"account_name": f"{root} - QB",
							"root_type": root,
							"is_group": "1",
							"company": self.company,
						}
					).insert(ignore_mandatory=True)
			except Exception as e:
				self._log_error(e, root)
		frappe.db.commit()

	def _migrate_entries(self, entity):
		try:
			query_uri = f"{self.api_endpoint}/company/{self.quickbooks_company_id}/query"
			max_result_count = 1000
			# Count number of entries
			response = self._get(query_uri, params={"query": f"""SELECT COUNT(*) FROM {entity}""", "minorversion": 73 })
			entry_count = response.json()["QueryResponse"]["totalCount"]

			# fetch pages and accumulate
			entries = []
			for start_position in range(1, entry_count + 1, max_result_count):
				response = self._get(
					query_uri,
					params={
						"query": """SELECT * FROM {} STARTPOSITION {} MAXRESULTS {}""".format(
							entity, start_position, max_result_count
						)
					},
				)
				entries.extend(response.json()["QueryResponse"][entity])
				# frappe.log_error(
				# 	title=f"QuickBooks Migration Log {entity}",
				# 	message="\n".join(
				# 		[
				# 			json.dumps(entries, sort_keys=True, indent=4, separators=(",", ": ")),
				# 		]
				# 	),
				# )
				frappe.log_error(
					title=f"QuickBooks Migration Log {entity} summary",
					message=response.text
				)
			entries = self._preprocess_entries(entity, entries)
			self._save_entries(entity, entries)
		except Exception as e:
			self._log_error(e, response.text)

	def _fetch_general_ledger(self):
		try:
			query_uri = f"{self.api_endpoint}/company/{self.quickbooks_company_id}/reports/GeneralLedger"
			response = self._get(
				query_uri,
				params={
					"columns": ",".join(["tx_date", "txn_type", "credit_amt", "debt_amt", 
                          "cust_name", "vend_name", "memo", "currency", "exch_rate", "debt_home_amt", "credit_home_amt"]),
					"date_macro": "All",
					"minorversion": 73,
				},
			)
			self.gl_entries = {}
			for section in response.json()["Rows"]["Row"]:
				if section["type"] == "Section":
					self._get_gl_entries_from_section(section)
			self.general_ledger = {}
			for account in self.gl_entries.values():
				for line in account:
					type_dict = self.general_ledger.setdefault(line["type"], {})
					if line["id"] not in type_dict:
						type_dict[line["id"]] = {
							"id": line["id"],
							"date": line["date"],
							"lines": [],
						}
					type_dict[line["id"]]["lines"].append(line)
			frappe.log_error(
					title=f"QuickBooks Migration Log GL",
					message="\n".join(
						[
							json.dumps(self.general_ledger, indent=4, separators=(",", ": ")),
						]
					),
				)
		except Exception as e:
			self._log_error(e, response.text)

	def _create_fiscal_years(self):
		try:
			# Assumes that exactly one fiscal year has been created so far
			# Creates fiscal years till oldest ledger entry date is covered
			from itertools import chain

			from frappe.utils.data import add_years, getdate

			smallest_ledger_entry_date = getdate(
				min(entry["date"] for entry in chain(*self.gl_entries.values()) if entry["date"])
			)
			oldest_fiscal_year = frappe.get_all(
				"Fiscal Year", fields=["year_start_date", "year_end_date"], order_by="year_start_date"
			)[0]
			# Keep on creating fiscal years
			# until smallest_ledger_entry_date is no longer smaller than the oldest fiscal year's start date
			while smallest_ledger_entry_date < oldest_fiscal_year.year_start_date:
				new_fiscal_year = frappe.get_doc({"doctype": "Fiscal Year"})
				new_fiscal_year.year_start_date = add_years(oldest_fiscal_year.year_start_date, -1)
				new_fiscal_year.year_end_date = add_years(oldest_fiscal_year.year_end_date, -1)
				if new_fiscal_year.year_start_date.year == new_fiscal_year.year_end_date.year:
					new_fiscal_year.year = new_fiscal_year.year_start_date.year
				else:
					new_fiscal_year.year = "{}-{}".format(
						new_fiscal_year.year_start_date.year, new_fiscal_year.year_end_date.year
					)
				new_fiscal_year.save()
				oldest_fiscal_year = new_fiscal_year

			frappe.db.commit()
		except Exception as e:
			self._log_error(e)

	def _migrate_entries_from_gl(self, entity):
		if entity in self.general_ledger:
			entries = self.general_ledger[entity].values()
			total = len(entries)
			for index, entry in enumerate(entries, start=1):
				self._publish(
					{
						"event": "progress",
						"message": _("Saving {0}").format(entity),
						"count": index,
						"total": total,
					}
				)
				self._save_entity_as_je(entity, entry)
			# self._save_entries(entity, self.general_ledger[entity].values())

	def _save_entries(self, entity, entries):
		entity_method_map = {
			"Account": self._save_account,
			"TaxRate": self._save_tax_rate,
			"TaxCode": self._save_tax_code,
			"Preferences": self._save_preference,
			"Customer": self._save_customer,
			"Item": self._save_item,
			"Vendor": self._save_vendor,
			"Invoice": self._save_invoice,
			"CreditMemo": self._save_credit_memo,
			"SalesReceipt": self._save_sales_receipt,
			"RefundReceipt": self._save_refund_receipt,
			"JournalEntry": self._save_journal_entry,
			"Bill": self._save_bill,
			"VendorCredit": self._save_vendor_credit,
			"Payment": self._save_payment,
			"BillPayment": self._save_bill_payment,
			"Purchase": self._save_purchase,
			"Deposit": self._save_deposit,
			"Advance Payment": self._save_advance_payment,
			"Tax Payment": self._save_tax_payment,
			"Sales Tax Payment": self._save_tax_payment,
			"Purchase Tax Payment": self._save_tax_payment,
			"Inventory Qty Adjust": self._save_inventory_qty_adjust,
		}
		total = len(entries)
		for index, entry in enumerate(entries, start=1):
			self._publish(
				{
					"event": "progress",
					"message": _("Saving {0}").format(entity),
					"count": index,
					"total": total,
				}
			)
			entity_method_map[entity](entry)
		frappe.db.commit()

	def _preprocess_entries(self, entity, entries):
		entity_method_map = {
			"Account": self._preprocess_accounts,
			"TaxRate": self._preprocess_tax_rates,
			"TaxCode": self._preprocess_tax_codes,
		}
		preprocessor = entity_method_map.get(entity)
		if preprocessor:
			entries = preprocessor(entries)
		return entries

	def _get_gl_entries_from_section(self, section, account=None):
		if "Header" in section:
			if "id" in section["Header"]["ColData"][0]:
				account = self._get_account_name_by_id(section["Header"]["ColData"][0]["id"])
			elif "value" in section["Header"]["ColData"][0] and section["Header"]["ColData"][0]["value"]:
				# For some reason during migrating UK company, account id is not available.
				# preprocess_accounts retains name:account mapping in self.accounts
				# This mapping can then be used to obtain quickbooks_id for correspondong account
				# Rest is trivial

				# Some Lines in General Leder Report are shown under Not Specified
				# These should be skipped
				if section["Header"]["ColData"][0]["value"] == "Not Specified":
					return
				account_id = self.accounts[section["Header"]["ColData"][0]["value"]]["Id"]
				account = self._get_account_name_by_id(account_id)
		entries = []
		for row in section["Rows"]["Row"]:
			if row["type"] == "Data":
				data = row["ColData"]
				entries.append(
					{
						"account": account,
						"date": data[0]["value"],
						"type": data[1]["value"],
						"id": data[1].get("id"),
						"credit": frappe.utils.flt(data[2]["value"]),
						"debit": frappe.utils.flt(data[3]["value"]),
						"customer": data[4]["value"],
						"vendor": data[5]["value"],
						"memo": data[6]["value"],
						"currency": data[8]["value"],
						"exch_rate": data[7]["value"],
						"debt_home_amt": data[9]["value"],
      					"credit_home_amt": data[10]["value"],
					}
				)
			if row["type"] == "Section":
				self._get_gl_entries_from_section(row, account)
		self.gl_entries.setdefault(account, []).extend(entries)

	def _preprocess_accounts(self, accounts):
		self.accounts = {account["Name"]: account for account in accounts}
		for account in accounts:
			if any(acc["SubAccount"] and acc["ParentRef"]["value"] == account["Id"] for acc in accounts):
				account["is_group"] = 1
			else:
				account["is_group"] = 0
		return sorted(accounts, key=lambda account: int(account["Id"]))

	def _save_account(self, account):
		mapping = {
			"Bank": "Asset",
			"Other Current Asset": "Asset",
			"Fixed Asset": "Asset",
			"Other Asset": "Asset",
			"Accounts Receivable": "Asset",
			"Equity": "Equity",
			"Expense": "Expense",
			"Other Expense": "Expense",
			"Cost of Goods Sold": "Expense",
			"Accounts Payable": "Liability",
			"Credit Card": "Liability",
			"Long Term Liability": "Liability",
			"Other Current Liability": "Liability",
			"Income": "Income",
			"Other Income": "Income",
		}
		# Map Quickbooks Account Types to ERPNext root_accunts and and root_type
		try:
			if not frappe.db.exists(
				{"doctype": "Account", "quickbooks_id": account["Id"], "company": self.company}
			):
				is_child = account["SubAccount"]
				is_group = account["is_group"]
				# Create Two Accounts for every Group Account
				if is_group:
					account_id = "Group - {}".format(account["Id"])
				else:
					account_id = account["Id"]

				if is_child:
					parent_account = self._get_account_name_by_id(
						"Group - {}".format(account["ParentRef"]["value"])
					)
				else:
					parent_account = encode_company_abbr(
						"{} - QB".format(mapping[account["AccountType"]]), self.company
					)

				frappe.get_doc(
					{
						"doctype": "Account",
						"quickbooks_id": account_id,
						"account_name": self._get_unique_account_name(account["Name"]),
						"root_type": mapping[account["AccountType"]],
						"account_type": self._get_account_type(account),
						"account_currency": account["CurrencyRef"]["value"],
						"parent_account": parent_account,
						"is_group": is_group,
						"company": self.company,
					}
				).insert()

				if is_group:
					# Create a Leaf account corresponding to the group account
					frappe.get_doc(
						{
							"doctype": "Account",
							"quickbooks_id": account["Id"],
							"account_name": self._get_unique_account_name(account["Name"]),
							"root_type": mapping[account["AccountType"]],
							"account_type": self._get_account_type(account),
							"account_currency": account["CurrencyRef"]["value"],
							"parent_account": self._get_account_name_by_id(account_id),
							"is_group": 0,
							"company": self.company,
						}
					).insert()
				if account.get("AccountSubType") == "UndepositedFunds":
					self.undeposited_funds_account = self._get_account_name_by_id(account["Id"])
					self.save()
		except Exception as e:
			self._log_error(e, account)

	def _get_account_type(self, account):
		account_subtype_mapping = {"UndepositedFunds": "Cash"}
		account_type = account_subtype_mapping.get(account.get("AccountSubType"))
		if account_type is None:
			account_type_mapping = {
				"Accounts Payable": "Payable",
				"Accounts Receivable": "Receivable",
				"Bank": "Bank",
				"Credit Card": "Bank",
			}
			account_type = account_type_mapping.get(account["AccountType"])
		return account_type

	def _preprocess_tax_rates(self, tax_rates):
		self.tax_rates = {tax_rate["Id"]: tax_rate for tax_rate in tax_rates}
		return tax_rates

	def _save_tax_rate(self, tax_rate):
		pass
		try:
			if not frappe.db.exists(
				{
					"doctype": "Account",
					"quickbooks_id": "TaxRate - {}".format(tax_rate["Id"]),
					"company": self.company,
				}
			):
				frappe.get_doc(
					{
						"doctype": "Account",
						"quickbooks_id": "TaxRate - {}".format(tax_rate["Id"]),
						"account_name": "{} - QB".format(tax_rate["Name"]),
						"root_type": "Liability",
						"account_type": "Tax",
						"tax_rate": tax_rate["RateValue"],
						"parent_account": encode_company_abbr("{} - QB".format("Liability"), self.company),
						"is_group": "0",
						"company": self.company,
					}
				).insert()
		except Exception as e:
			self._log_error(e, tax_rate)

	def _preprocess_tax_codes(self, tax_codes):
		self.tax_codes = {tax_code["Id"]: tax_code for tax_code in tax_codes}
		return tax_codes

	def _save_tax_code(self, tax_code):
		pass

	def _save_customer(self, customer):
		try:
			if not frappe.db.exists(
				{"doctype": "Customer", "quickbooks_id": customer["Id"], "company": self.company}
			):
				try:
					receivable_account = frappe.get_all(
						"Account",
						filters={
							"account_type": "Receivable",
							"account_currency": customer["CurrencyRef"]["value"],
							"company": self.company,
						},
					)[0]["name"]
				except Exception:
					receivable_account = None
				erpcustomer = frappe.get_doc(
					{
						"doctype": "Customer",
						"quickbooks_id": customer["Id"],
						"customer_name": customer["DisplayName"],
						"customer_type": "Individual",
						"customer_group": "Commercial",
						"default_currency": customer["CurrencyRef"]["value"],
						"accounts": [{"company": self.company, "account": receivable_account}],
						"territory": "All Territories",
						"company": self.company,
					}
				).insert()
				if "BillAddr" in customer:
					self._create_address(erpcustomer, "Customer", customer["BillAddr"], "Billing")
				if "ShipAddr" in customer:
					self._create_address(erpcustomer, "Customer", customer["ShipAddr"], "Shipping")
		except Exception as e:
			self._log_error(e, customer)

	def _save_item(self, item):
		try:
			if not frappe.db.exists(
				{"doctype": "Item", "item_name": item["FullyQualifiedName"], "company": self.company}
			):
				if item["Type"] in ("Service", "Inventory"):
					item_dict = {
						"doctype": "Item",
						"quickbooks_id": item["Id"],
						"item_code": item["FullyQualifiedName"],
						"description": item["Description"] if "Description" in item else item["FullyQualifiedName"],
						"stock_uom": "Unit",
						"is_stock_item": 0,
						"item_group": "All Item Groups",
						"company": self.company,
						"item_defaults": [
							{"company": self.company, "default_warehouse": self.default_warehouse}
						],
					}
					if "ExpenseAccountRef" in item:
						expense_account = self._get_account_name_by_id(item["ExpenseAccountRef"]["value"])
						item_dict["item_defaults"][0]["expense_account"] = expense_account
					if "IncomeAccountRef" in item:
						income_account = self._get_account_name_by_id(item["IncomeAccountRef"]["value"])
						item_dict["item_defaults"][0]["income_account"] = income_account
					frappe.get_doc(item_dict).insert()
		except Exception as e:
			self._log_error(e, item)

	def _allow_fraction_in_unit(self):
		frappe.db.set_value("UOM", "Unit", "must_be_whole_number", 0)

	def _save_vendor(self, vendor):
		try:
			if not frappe.db.exists(
				{"doctype": "Supplier", "quickbooks_id": vendor["Id"], "company": self.company}
			):
				try:
					payable_account = frappe.get_all(
						"Account",
						filters={
							"account_type": "Payable",
							"account_currency": vendor["CurrencyRef"]["value"],
							"company": self.company,
						},
					)[0]["name"]
				except Exception:
					payable_account = None
     
				erpsupplier = frappe.get_doc(
					{
						"doctype": "Supplier",
						"quickbooks_id": vendor["Id"],
						"supplier_name": vendor["DisplayName"],
						"supplier_group": "All Supplier Groups",
						"default_currency": vendor["CurrencyRef"]["value"],
						"accounts": [{"company": self.company, "account": payable_account}],
						"company": self.company,
					}
				).insert()
				if "BillAddr" in vendor:
					self._create_address(erpsupplier, "Supplier", vendor["BillAddr"], "Billing")
				if "ShipAddr" in vendor:
					self._create_address(erpsupplier, "Supplier", vendor["ShipAddr"], "Shipping")
		except Exception as e:
			self._log_error(e)

	def _save_preference(self, preference):
		try:
			if preference["SalesFormsPrefs"]["AllowShipping"]:
				default_shipping_account_id = preference["SalesFormsPrefs"]["DefaultShippingAccount"]
				self.default_shipping_account = self._get_account_name_by_id(
					self, default_shipping_account_id
				)
				self.save()
		except Exception as e:
			self._log_error(e, preference)

	def _save_invoice(self, invoice):
		# Invoice can be Linked with Another Transactions
		# If any of these transactions is a "StatementCharge" or "ReimburseCharge" then in the UI
		# item list is populated from the corresponding transaction, these items are not shown in api response
		# Also as of now there is no way of fetching the corresponding transaction from api
		# We in order to correctly reflect account balance make an equivalent Journal Entry
		quickbooks_id = "Invoice - {}".format(invoice["Id"])
		if any(
			linked["TxnType"] in ("StatementCharge", "ReimburseCharge") for linked in invoice["LinkedTxn"]
		):
			self._save_invoice_as_journal_entry(invoice, quickbooks_id)
		else:
			self._save_sales_invoice(invoice, quickbooks_id)

	def _save_credit_memo(self, credit_memo):
		# Credit Memo is equivalent to a return Sales Invoice
		quickbooks_id = "Credit Memo - {}".format(credit_memo["Id"])
		self._save_sales_invoice(credit_memo, quickbooks_id, is_return=True)

	def _save_sales_receipt(self, sales_receipt):
		# Sales Receipt is equivalent to a POS Sales Invoice
		quickbooks_id = "Sales Receipt - {}".format(sales_receipt["Id"])
		self._save_sales_invoice(sales_receipt, quickbooks_id, is_pos=True)

	def _save_refund_receipt(self, refund_receipt):
		# Refund Receipt is equivalent to a return POS Sales Invoice
		quickbooks_id = "Refund Receipt - {}".format(refund_receipt["Id"])
		self._save_sales_invoice(refund_receipt, quickbooks_id, is_return=True, is_pos=True)

	def _save_sales_invoice(self, invoice, quickbooks_id, is_return=False, is_pos=False):
		try:
			if not frappe.db.exists(
				{"doctype": "Sales Invoice", "quickbooks_id": quickbooks_id, "company": self.company}
			):
				invoice_dict = {
					"doctype": "Sales Invoice",
					"quickbooks_id": quickbooks_id,
					# Quickbooks uses ISO 4217 Code
					# of course this gonna come back to bite me
					"currency": invoice["CurrencyRef"]["value"],
					# Exchange Rate is provided if multicurrency is enabled
					# It is not provided if multicurrency is not enabled
					"conversion_rate": invoice.get("ExchangeRate", 1),
					"posting_date": invoice["TxnDate"],
					# QuickBooks doesn't make Due Date a mandatory field this is a hack
					"due_date": invoice.get("DueDate", invoice["TxnDate"]),
					"customer": frappe.get_all(
						"Customer",
						filters={
							"quickbooks_id": invoice["CustomerRef"]["value"],
							"company": self.company,
						},
					)[0]["name"],
					"items": self._get_si_items(invoice, is_return=is_return),
					"taxes": self._get_taxes(invoice),
					# Do not change posting_date upon submission
					"set_posting_time": 1,
					# QuickBooks doesn't round total
					"disable_rounded_total": 1,
					"is_return": is_return,
					"is_pos": is_pos,
					"payments": self._get_invoice_payments(invoice, is_return=is_return, is_pos=is_pos),
					"company": self.company,
				}
				discount = self._get_discount(invoice["Line"])
				if discount:
					if invoice["ApplyTaxAfterDiscount"]:
						invoice_dict["apply_discount_on"] = "Net Total"
					else:
						invoice_dict["apply_discount_on"] = "Grand Total"
					invoice_dict["discount_amount"] = discount["Amount"]

				invoice_doc = frappe.get_doc(invoice_dict)
				invoice_doc.insert()
				invoice_doc.submit()
		except Exception as e:
			self._log_error(e, [invoice, json.loads(invoice_doc.as_json())])

	def _get_si_items(self, invoice, is_return=False):
		items = []
		for line in invoice["Line"]:
			if line["DetailType"] == "SalesItemLineDetail":
				if line["SalesItemLineDetail"]["Qty"]==0:
					continue
				if line["SalesItemLineDetail"]["TaxCodeRef"]["value"] != "TAX":
					tax_code = line["SalesItemLineDetail"]["TaxCodeRef"]["value"]
				else:
					if "TxnTaxCodeRef" in invoice["TxnTaxDetail"]:
						tax_code = invoice["TxnTaxDetail"]["TxnTaxCodeRef"]["value"]
					else:
						tax_code = "NON"
				if line["SalesItemLineDetail"]["ItemRef"]["value"] != "SHIPPING_ITEM_ID":
					item_name = line["SalesItemLineDetail"]["ItemRef"]["name"]
					# match = re.match(r"^(.*):", item_name)
					# if match:
					# 	item_name = match.group(1)
					item = frappe.db.get_all(
						"Item",
						filters={
							"name": item_name,
							"company": self.company,
						},
						fields=["name", "stock_uom"],
					)[0]
					items.append(
						{
							"item_code": item["name"],
							"conversion_factor": 1,
							"uom": item["stock_uom"],
							"description": line.get(
								"Description", line["SalesItemLineDetail"]["ItemRef"]["name"]
							),
							"qty": line["SalesItemLineDetail"]["Qty"],
							"price_list_rate": line["SalesItemLineDetail"]["UnitPrice"],
							"cost_center": self.default_cost_center,
							"warehouse": self.default_warehouse,
							"item_tax_rate": json.dumps(self._get_item_taxes(tax_code)),
						}
					)
				else:
					items.append(
						{
							"item_name": "Shipping",
							"conversion_factor": 1,
							"expense_account": self._get_account_name_by_id(
								"TaxRate - {}".format(line["SalesItemLineDetail"]["TaxCodeRef"]["value"])
							),
							"uom": "Unit",
							"description": "Shipping",
							"income_account": self.default_shipping_account,
							"qty": 1,
							"price_list_rate": line["Amount"],
							"cost_center": self.default_cost_center,
							"warehouse": self.default_warehouse,
							"item_tax_rate": json.dumps(self._get_item_taxes(tax_code)),
						}
					)
				if is_return:
					items[-1]["qty"] *= -1
			elif line["DetailType"] == "DescriptionOnly":
				items[-1].update(
					{
						"margin_type": "Percentage",
						"margin_rate_or_amount": int(line["Description"].split("%")[0]),
					}
				)
		return items

	def _get_item_taxes(self, tax_code):
		tax_rates = self.tax_rates
		item_taxes = {}
		if tax_code != "NON":
			tax_code = self.tax_codes[tax_code]
			for rate_list_type in ("SalesTaxRateList", "PurchaseTaxRateList"):
				if rate_list_type in tax_code:
					for tax_rate_detail in tax_code[rate_list_type]["TaxRateDetail"]:
						if tax_rate_detail["TaxTypeApplicable"] == "TaxOnAmount":
							tax_head = self._get_account_name_by_id(
								"TaxRate - {}".format(tax_rate_detail["TaxRateRef"]["value"])
							)
							tax_rate = tax_rates[tax_rate_detail["TaxRateRef"]["value"]]
							item_taxes[tax_head] = tax_rate["RateValue"]
		return item_taxes

	def _get_invoice_payments(self, invoice, is_return=False, is_pos=False):
		if is_pos:
			amount = invoice["TotalAmt"]
			if is_return:
				amount = -amount
			return [
				{
					"mode_of_payment": "Cash",
					"account": self._get_account_name_by_id(invoice["DepositToAccountRef"]["value"]),
					"amount": amount,
				}
			]

	def _get_discount(self, lines):
		for line in lines:
			if line["DetailType"] == "DiscountLineDetail" and "Amount" in line["DiscountLineDetail"]:
				return line

	def _save_invoice_as_journal_entry(self, invoice, quickbooks_id):
		try:
			accounts = []
			for line in self.general_ledger["Invoice"][invoice["Id"]]["lines"]:
				account_line = {"account": line["account"], "cost_center": self.default_cost_center}
				if line["debit"]:
					account_line["debit_in_account_currency"] = line["debit"]
				elif line["credit"]:
					account_line["credit_in_account_currency"] = line["credit"]
				if frappe.db.get_value("Account", line["account"], "account_type") == "Receivable":
					account_line["party_type"] = "Customer"
					account_line["party"] = frappe.get_all(
						"Customer",
						filters={"quickbooks_id": invoice["CustomerRef"]["value"], "company": self.company},
					)[0]["name"]

				accounts.append(account_line)

			posting_date = invoice["TxnDate"]
			self.__save_journal_entry(quickbooks_id, accounts, posting_date)
		except Exception as e:
			self._log_error(e, [invoice, accounts])

	def _save_journal_entry(self, journal_entry):
		# JournalEntry is equivalent to a Journal Entry

		def _get_je_accounts(lines, trans_currency, exchange_rate):
			# Converts JounalEntry lines to accounts list
			posting_type_field_mapping = {
				"Credit": "credit_in_account_currency",
				"Debit": "debit_in_account_currency",
			}
			accounts = []
			for line in lines:
				if line["DetailType"] == "JournalEntryLineDetail":
					account_name = self._get_account_name_by_id(
						line["JournalEntryLineDetail"]["AccountRef"]["value"]
					)
					account_currency = frappe.db.get_value("Account", account_name, "account_currency")
					if account_currency == trans_currency:
						line_exchange_rate = exchange_rate
						line_amount = flt(line["Amount"])
					else:
						line_exchange_rate = 1
						line_amount = (flt(line["Amount"]) * flt(exchange_rate))

					posting_type = line["JournalEntryLineDetail"]["PostingType"]
					party=None
					party_type=None
					if account_name in frappe.get_all("Account", filters= [['account_type',"in", ['Payable', 'Receivable']]], pluck="name"):
						if "Entity" in line["JournalEntryLineDetail"]:
							if line["JournalEntryLineDetail"]["Entity"]["Type"]=="Vendor":
								party_type="Supplier"
							elif line["JournalEntryLineDetail"]["Entity"]["Type"]=="Customer":
								party_type="Customer"
							party =line["JournalEntryLineDetail"]["Entity"]["EntityRef"]["name"]
							match = re.match(r"^(.*):", party)
							if match:
								party = match.group(1)

					accounts.append(
						{
							"account": account_name,
							"party_type": party_type or None,
							"party": party or None,
							"exchange_rate": line_exchange_rate,
							"user_remark": line.get("Description", ''),
							posting_type_field_mapping[posting_type]: line_amount,
							"cost_center": self.default_cost_center,
						}
					)
			return accounts

		quickbooks_id = "Journal Entry - {}".format(journal_entry["Id"])
		accounts = _get_je_accounts(journal_entry["Line"], journal_entry["CurrencyRef"]["value"], journal_entry["ExchangeRate"])
		if "TaxLine" in journal_entry["TxnTaxDetail"]:
				for line in journal_entry["TxnTaxDetail"]["TaxLine"]:
					if line["Amount"]!=0:
						accounts.append(
							{
								"account": frappe.get_all("Account", 
									filters={
										"account_type": "Tax",
										"tax_rate": line["TaxLineDetail"]["TaxPercent"], 
										"company": self.company
									})[0]["name"],
								"debit_in_account_currency": line["Amount"],
								"cost_center": self.default_cost_center,
							}
						)
		posting_date = journal_entry["TxnDate"]

		self.__save_journal_entry(quickbooks_id, accounts, posting_date)

	def __save_journal_entry(self, quickbooks_id, accounts, posting_date):
		try:
			if not frappe.db.exists(
				{"doctype": "Journal Entry", "quickbooks_id": quickbooks_id, "company": self.company}
			):
				je = frappe.get_doc(
					{
						"doctype": "Journal Entry",
						"voucher_type": "Journal Entry",
						"quickbooks_id": quickbooks_id,
						"company": self.company,
						"posting_date": posting_date,
						"accounts": accounts,
						"multi_currency": 1,
					}
				)
				# total_debit = sum(flt((acc.get("debit_in_account_currency", 0) * flt(acc.get("exchange_rate", 1))),2) for acc in accounts)
				# total_credit = sum(flt((acc.get("credit_in_account_currency", 0) * flt(acc.get("exchange_rate", 1))),2) for acc in accounts)
				# difference = total_debit - total_credit
				# # frappe.log_error(title="Currency Diff", message=f"JE Currency Diff {total_debit}\n{total_credit}\n{difference}\n{je}")
				# if abs(difference):  # Set a threshold for minor differences
				# 	je.append("accounts",
				# 		{
				# 			"account": self._get_account_name_by_id(128),
				# 			"debit_in_account_currency": abs(difference) if difference < 0 else 0,
				# 			"credit_in_account_currency": abs(difference) if difference > 0 else 0,
				# 			"cost_center": self.default_cost_center,
				# 			"user_remark": "Rounding adjustment to balance debit/credit",
				# 		}
				# 	)
				je.insert()
				je.submit()
		except Exception as e:
			self._log_error(e, [accounts, json.loads(je.as_json())])

	def _save_bill(self, bill):
		# Bill is equivalent to a Purchase Invoice
		quickbooks_id = "Bill - {}".format(bill["Id"])
		self.__save_purchase_invoice(bill, quickbooks_id)

	def _save_vendor_credit(self, vendor_credit):
		# Vendor Credit is equivalent to a return Purchase Invoice
		quickbooks_id = "Vendor Credit - {}".format(vendor_credit["Id"])
		self.__save_purchase_invoice(vendor_credit, quickbooks_id, is_return=True)

	def __save_purchase_invoice(self, invoice, quickbooks_id, is_return=False):
		try:
			if not frappe.db.exists(
				{"doctype": "Purchase Invoice", "quickbooks_id": quickbooks_id, "company": self.company}
			):
				credit_to_account = self._get_account_name_by_id(invoice["APAccountRef"]["value"])
				invoice_dict = {
					"doctype": "Purchase Invoice",
					"quickbooks_id": quickbooks_id,
					"currency": invoice["CurrencyRef"]["value"],
					"conversion_rate": invoice.get("ExchangeRate", 1),
					"posting_date": invoice["TxnDate"],
					"due_date": invoice.get("DueDate", invoice["TxnDate"]),
					"credit_to": credit_to_account,
					"supplier": frappe.get_all(
						"Supplier",
						filters={
							"quickbooks_id": invoice["VendorRef"]["value"],
							"company": self.company,
						},
					)[0]["name"],
					"items": self._get_pi_items(invoice, is_return=is_return),
					"taxes": self._get_taxes(invoice),
					"set_posting_time": 1,
					"disable_rounded_total": 1,
					"is_return": is_return,
					"update_stock": 0,
					"company": self.company,
				}
				invoice_doc = frappe.get_doc(invoice_dict)
				# frappe.log_error(
				# 	title="QuickBooks Migration Log PI",
				# 	message=
				# 			json.dumps(invoice_dict, sort_keys=True, indent=4, separators=(",", ": ")))
				invoice_doc.insert()
				invoice_doc.submit()
		except Exception as e:
			self._log_error(e, [invoice, invoice_dict, json.loads(invoice_doc.as_json())])

	def _get_pi_items(self, purchase_invoice, is_return=False):
		items = []
		for line in purchase_invoice["Line"]:
			if line["DetailType"] == "ItemBasedExpenseLineDetail":
				if line["ItemBasedExpenseLineDetail"]["Qty"]==0:
					continue
				if line["ItemBasedExpenseLineDetail"]["TaxCodeRef"]["value"] != "TAX":
					tax_code = line["ItemBasedExpenseLineDetail"]["TaxCodeRef"]["value"]
				else:
					if "TxnTaxCodeRef" in purchase_invoice["TxnTaxDetail"]:
						tax_code = purchase_invoice["TxnTaxDetail"]["TxnTaxCodeRef"]["value"]
					else:
						tax_code = "NON"
				item_name=line["ItemBasedExpenseLineDetail"]["ItemRef"]["name"]
				# match = re.match(r"^[^:]*:(.*)", item_name)
				# if match:
				# 	item_name = match.group(1)
				item = frappe.db.get_all(
					"Item",
					filters={
						"Name": item_name,
						"company": self.company,
					},
					fields=["name", "stock_uom"],
				)[0]
				items.append(
					{
						"item_code": item["name"],
						"conversion_factor": 1,
						"uom": item["stock_uom"],
						"description": line.get(
							"Description", line["ItemBasedExpenseLineDetail"]["ItemRef"]["name"]
						),
						"qty": line["ItemBasedExpenseLineDetail"]["Qty"],
						"price_list_rate": line["ItemBasedExpenseLineDetail"]["UnitPrice"],
						"warehouse": self.default_warehouse,
						"cost_center": self.default_cost_center,
						"item_tax_rate": json.dumps(self._get_item_taxes(tax_code)),
					}
				)
			elif line["DetailType"] == "AccountBasedExpenseLineDetail":
				# continue
				if line["AccountBasedExpenseLineDetail"]["TaxCodeRef"]["value"] != "TAX":
					tax_code = line["AccountBasedExpenseLineDetail"]["TaxCodeRef"]["value"]
				else:
					if "TxnTaxCodeRef" in purchase_invoice["TxnTaxDetail"]:
						tax_code = purchase_invoice["TxnTaxDetail"]["TxnTaxCodeRef"]["value"]
					else:
						tax_code = "NON"
				items.append(
					{
						"item_name": line.get(
							"Description", line["AccountBasedExpenseLineDetail"]["AccountRef"]["name"]
						),
						"conversion_factor": 1,
						"expense_account": self._get_account_name_by_id(
							line["AccountBasedExpenseLineDetail"]["AccountRef"]["value"]
						),
						"uom": "Unit",
						"description": line.get(
							"Description", line["AccountBasedExpenseLineDetail"]["AccountRef"]["name"]
						),
						"qty": 1,
						"price_list_rate": line["Amount"],
						"warehouse": self.default_warehouse,
						"cost_center": self.default_cost_center,
						"item_tax_rate": json.dumps(self._get_item_taxes(tax_code)),
					}
				)
			if is_return:
				items[-1]["qty"] *= -1
		return items

	def _save_payment(self, payment):
		try:
			quickbooks_id = "Payment - {}".format(payment["Id"])
			# If DepositToAccountRef is not set on payment that means it actually doesn't affect any accounts
			# No need to record such payment
			# Such payment record is created QuickBooks Payments API
			if "DepositToAccountRef" not in payment:
				return

			# A Payment can be linked to multiple transactions
			accounts = []
			credit_amt = 0
			for line in payment["Line"]:
				linked_transaction = line["LinkedTxn"][0]
				if linked_transaction["TxnType"] == "Invoice":
					si_quickbooks_id = "Invoice - {}".format(linked_transaction["TxnId"])
					# Invoice could have been saved as a Sales Invoice or a Journal Entry
					if frappe.db.exists(
						{
							"doctype": "Sales Invoice",
							"quickbooks_id": si_quickbooks_id,
							"company": self.company,
						}
					):
						sales_invoice = frappe.get_all(
							"Sales Invoice",
							filters={
								"quickbooks_id": si_quickbooks_id,
								"company": self.company,
							},
							fields=["name", "customer", "debit_to", "grand_total"],
						)[0]
						reference_type = "Sales Invoice"
						reference_name = sales_invoice["name"]
						party = sales_invoice["customer"]
						party_account = sales_invoice["debit_to"]
						# total_amount = sales_invoice["grand_total"]

					if frappe.db.exists(
						{
							"doctype": "Journal Entry",
							"quickbooks_id": si_quickbooks_id,
							"company": self.company,
						}
					):
						journal_entry = frappe.get_doc(
							"Journal Entry",
							{
								"quickbooks_id": si_quickbooks_id,
								"company": self.company,
							},
						)
						# Invoice saved as a Journal Entry must have party and party_type set on line containing Receivable Account
						customer_account_line = next(
							filter(lambda acc: acc.party_type == "Customer", journal_entry.accounts)
						)

						reference_type = "Journal Entry"
						reference_name = journal_entry.name
						party = customer_account_line.party
						party_account = customer_account_line.account
					line_amount = line["Amount"]
					outstanding_amount =frappe.get_all(
							"Sales Invoice",
							filters={
								"quickbooks_id": si_quickbooks_id,
							},
							fields=["outstanding_amount"],
						)[0]["outstanding_amount"]					
					conversion_rate =frappe.get_all(
							"Sales Invoice",
							filters={
								"quickbooks_id": si_quickbooks_id,
							},
							fields=["conversion_rate"],
						)[0]["conversion_rate"]
					if line_amount > outstanding_amount:
						line_amount = outstanding_amount
					payment_currency = payment["CurrencyRef"]["value"]
					account_currency = frappe.db.get_value("Account", party_account, "account_currency")
					credit_exchange_rate = payment["ExchangeRate"] if account_currency != payment_currency else 1
					# credit_amt = credit_amt + math.floor((line_amount * conversion_rate)*100)/100
					credit_amt = credit_amt + flt((line_amount * conversion_rate), 2)
					accounts.append(
						{
							"party_type": "Customer",
							"party": party,
							"reference_type": reference_type,
							"reference_name": reference_name,
							"account": party_account,
							"exchange_rate": payment["ExchangeRate"] if account_currency == payment_currency else 1,
							"credit_in_account_currency": flt(line_amount * credit_exchange_rate, 2),
							"cost_center": self.default_cost_center,
						}
					)
		
			deposit_account = self._get_account_name_by_id(payment["DepositToAccountRef"]["value"])
			account_currency = frappe.db.get_value("Account", deposit_account, "account_currency")
			debit_exchange_rate = payment["ExchangeRate"] if account_currency != payment_currency else 1
			accounts.append(
				{
					"account": deposit_account,
					"debit_in_account_currency": flt(payment["TotalAmt"] * flt(debit_exchange_rate), 2),
					"exchange_rate": payment["ExchangeRate"] if account_currency == payment_currency else 1,
					"cost_center": self.default_cost_center,
				}
			)
			gl_payment = self.general_ledger["Payment"]
			for line in gl_payment[payment["Id"]]["lines"]:
				if "Exchange Gain or Loss - QB - NX" == line["account"]:
					accounts.append(
						{
							"account": self._get_account_name_by_id(95),
							"debit_in_account_currency": flt(line["debt_home_amt"], 2) or 0,
							"credit_in_account_currency": flt(line["credit_home_amt"], 2) or 0,
							"cost_center": self.default_cost_center,
							"user_remark": "Rounding adjustment to balance debit/credit",
						}
					)

			posting_date = payment["TxnDate"]
			self.__save_journal_entry(quickbooks_id, accounts, posting_date)
		except Exception as e:
			self._log_error(e, [payment, accounts])

	def _save_bill_payment(self, bill_payment):
		try:
			quickbooks_id = "BillPayment - {}".format(bill_payment["Id"])
			# A BillPayment can be linked to multiple transactions
			accounts = []
			debit_amt = 0
			for line in bill_payment["Line"]:
				linked_transaction = line["LinkedTxn"][0]
				if linked_transaction["TxnType"] == "Bill":
					pi_quickbooks_id = "Bill - {}".format(linked_transaction["TxnId"])
					if frappe.db.exists(
						{
							"doctype": "Purchase Invoice",
							"quickbooks_id": pi_quickbooks_id,
							"company": self.company,
						}
					):
						purchase_invoice = frappe.get_all(
							"Purchase Invoice",
							filters={
								"quickbooks_id": pi_quickbooks_id,
								"company": self.company,
							},
							fields=["name", "supplier", "credit_to", "grand_total", "conversion_rate"],
						)[0]
						reference_type = "Purchase Invoice"
						reference_name = purchase_invoice["name"]
						party = purchase_invoice["supplier"]
						party_account = purchase_invoice["credit_to"]
						total_amount = purchase_invoice["grand_total"]
						conversion_rate = purchase_invoice["conversion_rate"]
					account_currency = frappe.db.get_value("Account", party_account, "account_currency")
					outstanding_amount =frappe.get_all(
							"Purchase Invoice",
							filters={
								"quickbooks_id": pi_quickbooks_id,
							},
							fields=["outstanding_amount"],
						)[0]["outstanding_amount"]				
					line_amount = line["Amount"]
					conversion_rate = frappe.get_value("Purchase Invoice", {"quickbooks_id": pi_quickbooks_id}, "conversion_rate") or 1
					if line_amount>outstanding_amount:
						line_amount=outstanding_amount
					if account_currency != bill_payment["CurrencyRef"]["value"]:
						line_exchange_rate = conversion_rate
					else:
						line_exchange_rate = 1
					debit_amt = debit_amt + flt(line_amount * conversion_rate, 2)
					accounts.append(
						{
							"party_type": "Supplier",
							"party": party,
							"reference_type": reference_type,
							"reference_name": reference_name,
							"account": party_account,
							"debit_in_account_currency": flt(line_amount*line_exchange_rate, 2),
							"cost_center": self.default_cost_center,
						}
					)
				elif linked_transaction["TxnType"] != "Bill":
					je_quickbooks_id = "{} - {}".format(linked_transaction["TxnType"], linked_transaction["TxnId"])
					if frappe.db.exists(
						{
							"doctype": "Journal Entry",
							"quickbooks_id": je_quickbooks_id,
							"company": self.company,
						}
					):
						journal_entry = frappe.get_doc(
							"Journal Entry",
							{
								"quickbooks_id": je_quickbooks_id,
								"company": self.company,
							},
						)
						supplier_account_line = next(
							filter(lambda acc: acc.party_type == "Supplier", journal_entry.accounts)
						)
						reference_type = "Journal Entry"
						reference_name = journal_entry.name
						party = supplier_account_line.party
						party_account = supplier_account_line.account
					line_amount = line["Amount"]
					debit_amt = debit_amt + line_amount
					accounts.append(
						{
							"party_type": "Supplier",
							"party": party,
							"reference_type": reference_type,
							"reference_name": reference_name,
							"account": party_account,
							"debit_in_account_currency": line_amount,
							"cost_center": self.default_cost_center,
						}
					)
					
			if bill_payment["PayType"] == "Check":
				bank_account_id = bill_payment["CheckPayment"]["BankAccountRef"]["value"] if "BankAccountRef" in bill_payment["CheckPayment"] else None
			elif bill_payment["PayType"] == "CreditCard":
				bank_account_id = bill_payment["CreditCardPayment"]["CCAccountRef"]["value"]
			
			bank_account = self._get_account_name_by_id(bank_account_id)
			account_currency = frappe.db.get_value("Account", bank_account, "account_currency")
			bpayment_currency = bill_payment["CurrencyRef"]["value"]
			exchange_rate =  bill_payment["ExchangeRate"] if account_currency != bpayment_currency else 1

			accounts.append(
				{
					"account": bank_account,
					"credit_in_account_currency": flt(bill_payment["TotalAmt"]*exchange_rate, 2),
					"cost_center": self.default_cost_center,
				}
			)
			gl_payment = self.general_ledger["Bill Payment (Cheque)"]
			for line in gl_payment[bill_payment["Id"]]["lines"]:
				if "Exchange Gain or Loss - QB - NX" == line["account"]:
					accounts.append(
						{
							"account": self._get_account_name_by_id(95),
							"debit_in_account_currency": flt(line["debt_home_amt"]) or 0,
							"credit_in_account_currency": flt(line["credit_home_amt"]) or 0,
							"cost_center": self.default_cost_center,
							"user_remark": "Rounding adjustment to balance debit/credit",
						}
					)
			
			posting_date = bill_payment["TxnDate"]
			self.__save_journal_entry(quickbooks_id, accounts, posting_date)
		except Exception as e:
			self._log_error(e, [bill_payment, accounts])

	def _save_purchase(self, purchase):
		try:
			quickbooks_id = "Purchase - {}".format(purchase["Id"])

			account_name = self._get_account_name_by_id(purchase["AccountRef"]["value"])
			purchase_currency = purchase["CurrencyRef"]["value"]
			account_currency = frappe.db.get_value("Account", account_name, "account_currency")
			exchange_rate = purchase["ExchangeRate"] if account_currency != purchase_currency else 1
			
			# Credit Bank Account
			accounts = [
				{
					"account": account_name,
					"exchange_rate": purchase["ExchangeRate"] if account_currency == purchase_currency else 1,
					"credit_in_account_currency": flt(purchase["TotalAmt"]) * flt(exchange_rate),
					"cost_center": self.default_cost_center,
					"user_remark": purchase.get("PrivateNote", '')
				}
			]

			# Debit Mentioned Accounts
			for line in purchase["Line"]:
				if line["DetailType"] == "AccountBasedExpenseLineDetail":
					account = self._get_account_name_by_id(
						line["AccountBasedExpenseLineDetail"]["AccountRef"]["value"]
					)
				elif line["DetailType"] == "ItemBasedExpenseLineDetail":
					account = (
						frappe.get_doc(
							"Item",
							{
								"quickbooks_id": line["ItemBasedExpenseLineDetail"]["ItemRef"]["value"],
								"company": self.company,
							},
						)
						.item_defaults[0]
						.expense_account
					)
				account_currency = frappe.db.get_value("Account", account, "account_currency")
				exchange_rate = purchase["ExchangeRate"] if account_currency != purchase_currency else 1
				if line["Amount"]!= 0:
					accounts.append(
						{
							"account": account,
							"exchange_rate": purchase["ExchangeRate"] if account_currency == purchase_currency else 1,
							"debit_in_account_currency": flt(line["Amount"]) * flt(exchange_rate),
							"cost_center": self.default_cost_center,
							"user_remark": line.get("Description", '')
						}
					)

			# Debit Tax Accounts
			if "TxnTaxDetail" in purchase:
				for line in purchase["TxnTaxDetail"]["TaxLine"]:
					if line["Amount"]!=0:
						accounts.append(
							{
								"account": frappe.get_all("Account", filters = {
										"account_type": "Tax",
										"tax_rate": line["TaxLineDetail"]["TaxPercent"],
										"company": self.company
									})[0]["name"],
								"debit_in_account_currency": flt(line["Amount"]) * flt(purchase["ExchangeRate"]),
								"cost_center": self.default_cost_center,
							}
						)

			# If purchase["Credit"] is set to be True then it represents a refund
			if purchase.get("Credit"):
				for account in accounts:
					if "debit_in_account_currency" in account:
						account["credit_in_account_currency"] = account["debit_in_account_currency"]
						del account["debit_in_account_currency"]
					else:
						account["debit_in_account_currency"] = account["credit_in_account_currency"]
						del account["credit_in_account_currency"]

			posting_date = purchase["TxnDate"]
			
			self.__save_journal_entry(quickbooks_id, accounts, posting_date)
		except Exception as e:
			self._log_error(e, [purchase, accounts])

	def _save_deposit(self, deposit):
		try:
			quickbooks_id = "Deposit - {}".format(deposit["Id"])

			account_name = self._get_account_name_by_id(deposit["DepositToAccountRef"]["value"])
			deposit_currency = deposit["CurrencyRef"]["value"]
			account_currency = frappe.db.get_value("Account", account_name, "account_currency")
			exchange_rate = deposit["ExchangeRate"] if account_currency != deposit_currency else 1

			# Debit Bank Account
			accounts = [
				{
					"account": account_name,
					"exchange_rate": deposit["ExchangeRate"] if account_currency == deposit_currency else 1,
					"debit_in_account_currency": flt(deposit["TotalAmt"]) * flt(exchange_rate),
					"cost_center": self.default_cost_center,
					"user_remark": deposit.get("PrivateNote", '')
				}
			]

			# Credit Mentioned Accounts
			for line in deposit["Line"]:
				if "LinkedTxn" in line:
					accounts.append(
						{
							"account": self.undeposited_funds_account,
							"credit_in_account_currency": line["Amount"],
							"cost_center": self.default_cost_center,
						}
					)
				else:
					account_name = self._get_account_name_by_id(
								line["DepositLineDetail"]["AccountRef"]["value"]
							)
					account_currency = frappe.db.get_value("Account", account_name, "account_currency")
					exchange_rate = deposit["ExchangeRate"] if account_currency != deposit_currency else 1
					party=None
					party_type=None
					if account_name in frappe.get_all("Account", filters= [['account_type',"in", ['Payable', 'Receivable']]], pluck="name"):
						if "Entity" in line["DepositLineDetail"]:
							if line["DepositLineDetail"]["Entity"]["type"].upper()=="VENDOR":
								party_type="Supplier"
							elif line["DepositLineDetail"]["Entity"]["Type"].upper()=="CUSTOMER":
								party_type="Customer"
							party =line["DepositLineDetail"]["Entity"]["name"]
							match = re.match(r"^(.*):", party)
							if match:
								party = match.group(1)
					accounts.append(
						{
							"account": account_name,
							"exchange_rate": deposit["ExchangeRate"] if account_currency == deposit_currency else 1,
							"credit_in_account_currency": flt(line["Amount"]) * flt(exchange_rate),
							"party_type": party_type,
							"party":party,
							"cost_center": self.default_cost_center,
							"user_remark": line.get("Description", '')
						}
					)

			# Debit Cashback if mentioned
			if "CashBack" in deposit:
				accounts.append(
					{
						"account": self._get_account_name_by_id(deposit["CashBack"]["AccountRef"]["value"]),
						"debit_in_account_currency": deposit["CashBack"]["Amount"],
						"cost_center": self.default_cost_center,
					}
				)

			posting_date = deposit["TxnDate"]
			self.__save_journal_entry(quickbooks_id, accounts, posting_date)
		except Exception as e:
			self._log_error(e, [deposit, accounts])

	def _save_advance_payment(self, advance_payment):
		quickbooks_id = "Advance Payment - {}".format(advance_payment["id"])
		self.__save_ledger_entry_as_je(advance_payment, quickbooks_id)

	def _save_tax_payment(self, tax_payment):
		quickbooks_id = "Tax Payment - {}".format(tax_payment["id"])
		self.__save_ledger_entry_as_je(tax_payment, quickbooks_id)

	def _save_inventory_qty_adjust(self, inventory_qty_adjust):
		quickbooks_id = "Inventory Qty Adjust - {}".format(inventory_qty_adjust["id"])
		self.__save_ledger_entry_as_je(inventory_qty_adjust, quickbooks_id)

	def _save_entity_as_je(self, entity_type, entity):
		if entity_type== "Bill Payment (Cheque)":
			quickbooks_id = "BillPayment - {}".format( entity["id"])
		else:
			quickbooks_id = "{} - {}".format(entity_type, entity["id"])
		self.__save_ledger_entry_as_je(entity, quickbooks_id)

	# # def __save_ledger_entry_as_je(self, ledger_entry, quickbooks_id):
	# 	try:
	# 		accounts = []
	# 		total_debit_company_currency = 0
	# 		total_credit_company_currency = 0
	# 		company_currency = get_company_currency(self.company)

	# 		for line in ledger_entry["lines"]:
	# 			if (line['credit'] == 0 and line['debit'] == 0 and
    #     			flt(line['credit_home_amt']) == 0 and flt(line['debt_home_amt']) == 0):
	# 				continue
	# 			account_line = {"account": line["account"], "cost_center": self.default_cost_center, "user_remark": line["memo"]}
	# 			if line["vendor"] and line["account"] in frappe.get_all("Account", filters={"account_type": "Payable"}, pluck="name"):
	# 				account_line["party_type"] = "Supplier"
	# 				match = re.match(r"^(.*):", line["vendor"])
	# 				if match:
	# 					account_line["party"] = match.group(1)
	# 				else:
	# 					account_line["party"] = line["vendor"]
	# 			elif line["customer"] and line["account"] in frappe.get_all("Account", filters={"account_type": "Receivable"}, pluck="name"):
	# 				account_line["party_type"] = "Customer"
	# 				match = re.match(r"^(.*):", line["customer"])
	# 				if match:
	# 					account_line["party"] = match.group(1)
	# 				else:
	# 					account_line["party"] = line["customer"]
     
	# 			account_currency = frappe.db.get_value("Account", line["account"], "account_currency")
				
	# 			if line["credit"] != 0 or line["debit"] != 0:
	# 				amount_type = "credit" if line["credit"] else "debit"
	# 			else:
	# 				amount_type = "credit" if flt(line["credit_home_amt"]) else "debit"
	# 			amount_home_key = "credit_home_amt" if amount_type == "credit" else "debt_home_amt"
	# 			account_line_key = f"{amount_type}_in_account_currency"

	# 			# Assign amount in account currency
	# 			if line["currency"] == account_currency:
	# 				account_line[account_line_key] = line[amount_type] if line[amount_type] else flt(line[amount_home_key])
	# 			else:
	# 				account_line[account_line_key] = flt(line[amount_home_key])

	# 			# Set exchange rate if currencies differ
	# 			if company_currency == account_currency:
	# 				if amount_type == "debit":
	# 					total_debit_company_currency = total_debit_company_currency + account_line[account_line_key]
	# 				else:
	# 					total_credit_company_currency = total_credit_company_currency + account_line[account_line_key]

	# 			accounts.append(account_line)

	# 		for account_line in accounts:
	# 			if company_currency != frappe.db.get_value("Account", account_line["account"], "account_currency"):
	# 				if "credit_in_account_currency" in account_line:
	# 					amount_type = "credit_in_account_currency"
	# 				else:
	# 					amount_type = "debit_in_account_currency"
	# 				account_line["exchange_rate"] = abs(total_debit_company_currency-total_credit_company_currency) / account_line[amount_type]
	# 		posting_date = ledger_entry["date"]
	# 		if accounts:
	# 			self.__save_journal_entry(quickbooks_id, accounts, posting_date)
	# 	except Exception as e:
	# 		self._log_error(e, ledger_entry)

	def __save_ledger_entry_as_je(self, ledger_entry, quickbooks_id):
		accounts_map = {
		"A. Majid Rumani Loan Account - QB - NX": "Short Term Investment:A. Majid Rumani Loan Account - NX",
		"Majeed Bhai Loan Account - QB - NX": "Short Term Investment:Majeed Bhai Loan Account - NX",
		"Nasir Rais - Short Term Borrowings - QB - NX":"Nasir Rais - Short Term Borrowings - NX",
		"Ankit Sinha - Short Term Borrowings - QB - NX":"Ankit Sinha - Short Term Borrowings - NX",
		"Brijesh Loan Account - QB - NX":"Short Term Investment:Brijesh Loan Account - NX",
		"ADCB 12033795820001 AED02 - QB - NX": "ADCB 12033795820001 AED02 - NX",
		"ADCB 12033795920001 AED01 - QB - NX": "ADCB 12033795920001 AED01 - NX",
		"ADCB Loan - QB - NX": "ADCB Loan - NX",
		"ADCB USD (IN AED) 23033795830001 - QB - NX": "ADCB USD (IN AED) 23033795830001 - NX",
		"Accounts Payable (A/P) - QB - NX": "Creditors - NX",
		"Accounts Payable (A/P) - USD - QB - NX": "Creditors USD - NX",
		"Accounts Payable (A/P) - INR - QB - NX": "Creditors INR - NX",
		"Accounts Payable (A/P) - OMR - QB - NX": "Creditors OMR - NX",
		"Accounts Payable (A/P) - EUR - QB - NX": "Creditors EUR - NX",
		"Accounts Payable (A/P) - GBP - QB - NX": "Creditors GBP - NX",
		"Accounts Payable (A/P) - KWD - QB - NX": "Creditors KWD - NX",
		"Accounts Payable (A/P) - QAR - QB - NX": "Creditors QAR - NX",
		"Office Expense - QATAR - QB - NX":"Office Expense - QATAR - NX",
		"Accounts Receivable (A/R) - QB - NX": "Debtors - NX",
		"Accounts Receivable (A/R) - USD - QB - NX": "Debtors USD - NX",
		"Akbar Loan Account - QB - NX": "Short Term Investment:Akbar Loan Account - NX",
		"Appliances And Devices - Accumulated Depreciation - QB - NX": "Accumulated Depreciation - NX",
		"Arif Kandathil - QB - NX": "Payroll Expenses - NX",
		"BONUS PAID - QB - NX": "Bonus Paid - NX",
		"Bank charges - QB - NX": "Bank charges - NX",
		"CCTV Camera - Accumulated Depreciation - QB - NX": "Accumulated Depreciation - NX",
		"CONSUMABLES & TOOLS - QB - NX": "Consumables & Tools - NX",
		"Cash on hand - QB - NX": "Cash - NX",
		"Computer And Accessories - Accumulated Depreciation - QB - NX": "Accumulated Depreciation - NX",
		"Customs Charge - QB - NX": "Customs Charge - NX",
		"DISCOUNT RECEIVED - QB - NX": "Discount Received - NX",
		"Depreciation - Appliances And Devices - QB - NX": "Depreciation - Appliances And Devices - NX",
		"Depreciation - CCTV Camera - QB - NX": "Depreciation - CCTV Camera - NX",
		"Depreciation - Computer & Accessories - QB - NX": "Depreciation - Computer & Accessories - NX",
		"Depreciation - Furniture and Fixtures - QB - NX": "Depreciation - Furniture and Fixtures - NX",
		"Depreciation - Machinery and Equipment - QB - NX": "Depreciation - Machinery and Equipment - NX",
		"Depreciation - Motor Vehicle - QB - NX": "Depreciation - Motor Vehicle - NX",
		"Depreciation - Software - QB - NX": "Depreciation - Software - NX",
		"Dividend disbursed - QB - NX": "Dividend Disbursed - NX",
		"Dues and subscriptions - QB - NX": "Dues and subscriptions - NX",
		"Electricity & Water Expense - QB - NX": "Electricity & Water Expense - NX",
		"Exchange Gain or Loss - QB - NX": "Exchange Gain/Loss - NX",
		"FUEL EXPENSE - QB - NX": "Fuel Expense - NX",
		"Faisal Loan Account - QB - NX": "Short Term Investment:Faisal Loan Account - NX",
		"Furniture and Fixtures - Accumulated Depreciation - QB - NX": "Accumulated Depreciation - NX",
		"Furniture and Fixtures - QB - NX": "Furnitures and Fixtures - NX",
		"Gratuity Expense - QB - NX": "Gratuity Expense - NX",
		"Insurance - General - QB - NX": "Insurance - NX",
		"Interest expense - QB - NX": "Interest Expense - NX",
		"Interest income - QB - NX": "Interest income - NX",
		"Inventory - QB - NX": "Inventory - NX",
		"Junaid Bhai Loan Account - QB - NX": "Short Term Investment:Junaid Bhai Loan Account - NX",
		"Khaan Saab Loan Account - QB - NX": "Short Term Investment:Khaan Saab Loan Account - NX",
		"LOAN TO STAFF - QB - NX": "LOAN TO STAFF - NX",
		"Legal and professional fees - QB - NX": "Legal Expenses - NX",
		"Commissions and fees - QB - NX": "Commissions and fees - NX",
		"Machinery and equipment - Accumulated Depreciation - QB - NX": "Accumulated Depreciation - NX",
		"Machinery and equipment - QB - NX": "Machinery and equipment - NX",
		"PRELIMINARY EXPENSE - QB - NX": "Preliminary Expense - NX",
		"Computer And Accessories - QB - NX":"Computer And Accessories - NX",
		"Insurance - Disability - QB - NX":"Insurance - NX",
		"Utilities - QB - NX":'Utilities - NX',
		"Other Expense - QB - NX":"Other Expense - NX",
		"Other operating income (expenses) - QB - NX":"Other operating income (expenses) - NX",
		'Bad debts - QB - NX':"Bad debts - NX",
		"Other selling expenses - QB - NX": "Other selling expenses - NX",
		"Meals and entertainment - QB - NX": "Meals and entertainment - NX",
		"Motor Vehicle - Accumulated Depreciation - QB - NX": "Accumulated Depreciation - NX",
		"Motor Vehicle - QB - NX": "Motor Vehicle - NX",
		"Uncategorised Expense - QB - NX":"Uncategorised Expense - NX",
		"Uncategorised Income - QB - NX":"Uncategorised Income - NX",
		"OFFICE REFRESHMENTS - QB - NX": "Office Refreshments - NX",
		"Office expenses - QB - NX": "Office expenses - NX",
		"Other general and administrative expenses - QB - NX": "Other general and administrative expenses - NX",
		"Overhead - COS - QB - NX": "Overhead - COS - NX",
		"Payroll Expenses - 1 - QB - NX": "Payroll Expenses - NX",
		"Petty Cash - QB - NX": "Petty Cash - NX",
		"Prepaid Visa Expense - QB - NX": "Prepaid Visa Expense - NX",
		"Prepaid expenses - QB - NX": "Prepaid expenses - NX",
		"Provision for Gratuity - QB - NX": "Provision for Gratuity - NX",
		"Purchases - QB - NX": "Purchases - NX",
		"ROUNDOFF - QB - NX": "Round Off - NX",
		"Rent or lease payments - QB - NX": "Rent or lease payments - NX",
		"Repairs and Maintenance - QB - NX": "Repairs and Maintenance - NX",
		"Retained Earnings - QB - NX": "Retained Earnings - NX",
		"Salim Lakdawala Loan Account - QB - NX": "Short Term Investment:Salim Lakdawala Loan Account - NX",
		"Samiuddin Siddqi Loan Account - QB - NX": "Short Term Investment:Samiuddin Siddqi Loan Account - NX",
		"Security Deposits - QB - NX": "Security Deposits - NX",
		"Shipping and delivery expense - QB - NX": "Shipping and delivery - NX",
		"Short Term Investment - 1 - QB - NX": "Short Term Investment - NX",
		"Software - Accumulated Depreciation - QB - NX": "Accumulated Depreciation - NX",
		"Software - QB - NX": "Software - NX",
		"CCTV Camera - 1 - QB - NX":"CCTV Camera - NX",
		"Appliances And Devices - QB - NX":"Appliances And Devices - NX",
		"Staff Visa Expense - U.A.E - QB - NX": "Staff Visa Expense - U.A.E - NX",
		"Staff Welfare Expense - QB - NX": "Staff Welfare Expense - NX",
		"Stationery and printing - QB - NX": "Print and Stationery - NX",
		"Supplies - QB - NX": "Supplies - NX",
		"Suspense Account - QB - NX": "Suspense Account - NX",
		"Telephone expense - QB - NX": "Telephone Expenses - NX",
		"Travel expenses - general and admin expenses - QB - NX": "Travel expenses - general and admin expenses - NX",
		"VAT Control - QB - NX": "VAT Control - NX",
		"VAT Payable / Receivable - QB - NX": "VAT Payable / Receivable - NX",
		"VAT Suspense - QB - NX": "VAT Suspense - NX",
		"VISA CANCELLATION EXPENSE - QB - NX": "VISA CANCELLATION EXPENSE - NX",
		"VISIT VISA EXPENSE - QB - NX": "VISIT VISA EXPENSE - NX",
		"Waste Management Expense - QB - NX": "Waste Management Expense - NX",
		"Sales - QB - NX":"Sales - NX",
		"Discounts given - QB - NX": 'Discounts given - NX',
		"DESKTOP / LAPTOP - 1 - QB - NX":"DESKTOP / LAPTOP - NX",
		"AMC EXPENSES - QB - NX":"AMC EXPENSES - NX",
		"Website Development and Maintenance Charges - QB - NX":"Website Development and Maintenance Charges - NX",
		"AUDIT EXPENSE - QB - NX":"AUDIT EXPENSE - NX"
		}

		try:
			accounts = []
			total_debit_company_currency = 0
			total_credit_company_currency = 0
			company_currency = get_company_currency(self.company)

			for line in ledger_entry["lines"]:
				if (line['credit'] == 0 and line['debit'] == 0 and
        			flt(line['credit_home_amt']) == 0 and flt(line['debt_home_amt']) == 0):
					continue
				account_line = {"account": accounts_map[line["account"]], "cost_center": self.default_cost_center, "user_remark": line["memo"]}
				if line["vendor"] and line["account"] in frappe.get_all("Account", filters={"account_type": "Payable"}, pluck="name"):
					account_line["party_type"] = "Supplier"
					match = re.match(r"^(.*):", line["vendor"])
					if match:
						account_line["party"] = match.group(1)
					else:
						account_line["party"] = line["vendor"]
				elif line["customer"] and line["account"] in frappe.get_all("Account", filters={"account_type": "Receivable"}, pluck="name"):
					account_line["party_type"] = "Customer"
					match = re.match(r"^(.*):", line["customer"])
					if match:
						account_line["party"] = match.group(1)
					else:
						account_line["party"] = line["customer"]
     
				account_currency = frappe.db.get_value("Account", line["account"], "account_currency")
				
				if line["credit"] != 0 or line["debit"] != 0:
					amount_type = "credit" if line["credit"] else "debit"
				else:
					amount_type = "credit" if flt(line["credit_home_amt"]) else "debit"
				amount_home_key = "credit_home_amt" if amount_type == "credit" else "debt_home_amt"
				account_line_key = f"{amount_type}_in_account_currency"

				# Assign amount in account currency
				if line["currency"] == account_currency:
					account_line[account_line_key] = line[amount_type] if line[amount_type] else flt(line[amount_home_key])
				else:
					account_line[account_line_key] = flt(line[amount_home_key])

				# Set exchange rate if currencies differ
				if company_currency == account_currency:
					if amount_type == "debit":
						total_debit_company_currency = total_debit_company_currency + account_line[account_line_key]
					else:
						total_credit_company_currency = total_credit_company_currency + account_line[account_line_key]

				accounts.append(account_line)

			for account_line in accounts:
				if company_currency != frappe.db.get_value("Account", account_line["account"], "account_currency"):
					if "credit_in_account_currency" in account_line:
						amount_type = "credit_in_account_currency"
					else:
						amount_type = "debit_in_account_currency"
					account_line["exchange_rate"] = abs(total_debit_company_currency-total_credit_company_currency) / account_line[amount_type]
			posting_date = ledger_entry["date"]
			if accounts:
				self.__save_journal_entry(quickbooks_id, accounts, posting_date)
		except Exception as e:
			self._log_error(e, ledger_entry)

	def _get_taxes(self, entry):
		taxes = []
		if "TxnTaxDetail" not in entry or "TaxLine" not in entry["TxnTaxDetail"]:
			return taxes
		for line in entry["TxnTaxDetail"]["TaxLine"]:
			account_head = frappe.get_all("Account", filters={
				"account_type": "Tax",
				"tax_rate": line["TaxLineDetail"]["TaxPercent"],
				"company": self.company
			})[0]["name"]
			tax_rate = line["TaxLineDetail"]["TaxRateRef"]["value"]
			tax_type_applicable = self._get_tax_type(tax_rate)
			if tax_type_applicable == "TaxOnAmount":
				taxes.append(
					{
						"charge_type": "On Net Total",
						"account_head": account_head,
						"description": account_head,
						"cost_center": self.default_cost_center,
						"rate": line["TaxLineDetail"]["TaxPercent"],
					}
				)
			else:
				parent_tax_rate = self._get_parent_tax_rate(tax_rate)
				parent_row_id = self._get_parent_row_id(parent_tax_rate, taxes)
				taxes.append(
					{
						"charge_type": "On Previous Row Amount",
						"row_id": parent_row_id,
						"account_head": account_head,
						"description": account_head,
						"cost_center": self.default_cost_center,
						"rate": line["TaxLineDetail"]["TaxPercent"],
					}
				)
		return taxes

	def _get_tax_type(self, tax_rate):
		for tax_code in self.tax_codes.values():
			for rate_list_type in ("SalesTaxRateList", "PurchaseTaxRateList"):
				if rate_list_type in tax_code:
					for tax_rate_detail in tax_code[rate_list_type]["TaxRateDetail"]:
						if tax_rate_detail["TaxRateRef"]["value"] == tax_rate:
							return tax_rate_detail["TaxTypeApplicable"]

	def _get_parent_tax_rate(self, tax_rate):
		parent = None
		for tax_code in self.tax_codes.values():
			for rate_list_type in ("SalesTaxRateList", "PurchaseTaxRateList"):
				if rate_list_type in tax_code:
					for tax_rate_detail in tax_code[rate_list_type]["TaxRateDetail"]:
						if tax_rate_detail["TaxRateRef"]["value"] == tax_rate:
							parent = tax_rate_detail["TaxOnTaxOrder"]
					if parent:
						for tax_rate_detail in tax_code[rate_list_type]["TaxRateDetail"]:
							if tax_rate_detail["TaxOrder"] == parent:
								return tax_rate_detail["TaxRateRef"]["value"]

	def _get_parent_row_id(self, tax_rate, taxes):
		tax_account = self._get_account_name_by_id(f"TaxRate - {tax_rate}")
		for index, tax in enumerate(taxes):
			if tax["account_head"] == tax_account:
				return index + 1

	def _create_address(self, entity, doctype, address, address_type):
		try:
			if not frappe.db.exists({"doctype": "Address", "quickbooks_id": address["Id"]}) and address["Line1"]:
				frappe.get_doc(
					{
						"doctype": "Address",
						"quickbooks_address_id": address["Id"],
						"address_title": entity.name,
						"address_type": address_type,
						"address_line1": address["Line1"],
						"city": address["City"] if "City" in address else '',
						"emirate": address["City"].title() if address["country"] in 
											["UAE", "UAW", "United Arab Emirates"] and address["City"] else '',
						"pincode": address["PostalCode"] if address["PostalCode"] else '',
						"links": [{"link_doctype": doctype, "link_name": entity.name}],
					}
				).insert()
		except Exception as e:
			self._log_error(e, address)

	def _get(self, *args, **kwargs):
		kwargs["headers"] = {
			"Accept": "application/json",
			"Authorization": f"Bearer {self.access_token}",
		}
		response = requests.get(*args, **kwargs)
		# HTTP Status code 401 here means that the access_token is expired
		# We can refresh tokens and retry
		# However limitless recursion does look dangerous
		if response.status_code == 401:
			self._refresh_tokens()
			response = self._get(*args, **kwargs)
		return response

	def _get_account_name_by_id(self, quickbooks_id):
		return frappe.get_all("Account", filters={"quickbooks_id": quickbooks_id, "company": self.company})[
			0
		]["name"]

	def _publish(self, *args, **kwargs):
		frappe.publish_realtime("quickbooks_progress_update", *args, **kwargs, user=self.modified_by)

	def _get_unique_account_name(self, quickbooks_name, number=0):
		if number:
			quickbooks_account_name = f"{quickbooks_name} - {number} - QB"
		else:
			quickbooks_account_name = f"{quickbooks_name} - QB"
		company_encoded_account_name = encode_company_abbr(quickbooks_account_name, self.company)
		if frappe.db.exists(
			{"doctype": "Account", "name": company_encoded_account_name, "company": self.company}
		):
			unique_account_name = self._get_unique_account_name(quickbooks_name, number + 1)
		else:
			unique_account_name = quickbooks_account_name
		return unique_account_name

	def _log_error(self, execption, data=""):
		frappe.log_error(
			title="QuickBooks Migration Error",
			message="\n".join(
				[
					"Data",
					json.dumps(data, sort_keys=True, indent=4, separators=(",", ": ")),
					"Exception",
					traceback.format_exc(),
				]
			),
		)

	def set_indicator(self, status):
		self.status = status
		self.save()
		frappe.db.commit()