# fdp/trading/broker_adapter_directa.py
import asyncio
from typing import Dict
from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from fdp.trading.broker_adapter_enhanced import BaseBrokerAdapter, EnhancedOrder

class DirectaBrokerAdapter(BaseBrokerAdapter):
    def __init__(self, config, notifier, redis_client):
        super().__init__(config, notifier, redis_client)
        self.username = config.directa_username
        self.password = config.directa_password
        self.driver = None
    
    async def connect(self):
        try:
            self.driver = Driver(browser="chrome", headless=True)
            self.driver.get("https://lbo.directa.it")
            await self._login()
        except Exception as e:
            await self.notifier.send_alert(f"Directa connection failed: {str(e)}")
            raise
    
    async def _login(self):
        try:
            wait = WebDriverWait(self.driver, 10)
            wait.until(EC.presence_of_element_located((By.ID, "username"))).send_keys(self.username)
            self.driver.find_element(By.ID, "password").send_keys(self.password)
            self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
            wait.until(EC.url_contains("home"))
        except TimeoutException:
            raise Exception("Directa login timeout")
    
    async def disconnect(self):
        if self.driver:
            self.driver.quit()
    
    async def place_order(self, order: EnhancedOrder) -> str:
        try:
            self.driver.get("https://lbo.directa.it/Ordini/Insert")
            wait = WebDriverWait(self.driver, 10)
            
            wait.until(EC.presence_of_element_located((By.ID, "symbol"))).send_keys(order.symbol)
            await asyncio.sleep(1)
            
            action_select = wait.until(EC.presence_of_element_located((By.ID, "action")))
            action_select.send_keys("Acquista" if order.action == "buy" else "Vendi")
            
            quantity_input = self.driver.find_element(By.ID, "quantity")
            quantity_input.clear()
            quantity_input.send_keys(str(order.quantity))
            
            price_input = self.driver.find_element(By.ID, "price")
            price_input.clear()
            price_input.send_keys(str(order.limit_price))
            
            self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']").click()
            
            confirm = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "order-confirm")))
            order_id = confirm.get_attribute("data-order-id")
            
            return order_id
        except (TimeoutException, NoSuchElementException) as e:
            raise Exception(f"Order placement failed: {str(e)}")
    
    async def get_account_summary(self) -> Dict[str, float]:
        try:
            self.driver.get("https://lbo.directa.it/Portafoglio")
            wait = WebDriverWait(self.driver, 10)
            cash = float(wait.until(EC.presence_of_element_located((By.ID, "cash"))).text.replace(",", ""))
            portfolio = float(wait.until(EC.presence_of_element_located((By.ID, "portfolio_value"))).text.replace(",", ""))
            return {"cash": cash, "portfolio_value": portfolio}
        except Exception as e:
            return {"cash": 0, "portfolio_value": 0}
    
    async def get_positions(self) -> Dict[str, Dict]:
        try:
            self.driver.get("https://lbo.directa.it/Portafoglio")
            rows = self.driver.find_elements(By.CSS_SELECTOR, "table.positions tr")
            positions = {}
            for row in rows[1:]:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 3:
                    positions[cols[0].text] = {
                        "quantity": int(cols[1].text),
                        "market_value": float(cols[2].text.replace(",", ""))
                    }
            return positions
        except Exception:
            return {}
