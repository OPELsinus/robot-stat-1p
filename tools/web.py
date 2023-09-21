from contextlib import suppress
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Union

from pywinauto.timings import wait_until
from selenium import webdriver
from selenium.webdriver import ChromeOptions, ActionChains, Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.wait import WebDriverWait


class Web:
    keys = Keys

    class Element:
        keys = Keys

        def __init__(self, element, selector, by, driver):
            self.element: WebElement = element
            self.selector = selector
            self.by = by
            self.driver: WebDriver = driver

        def page_load(self, url, timeout=60):
            def body():
                return url != self.driver.current_url

            wait_until(timeout, 0.5, body)

        def scroll(self, delay=0):
            sleep(delay)
            ActionChains(self.driver).move_to_element(self.element).perform()

        def clear(self, delay=0):
            sleep(delay)
            self.element.clear()

        def click(self, double=False, delay=0, scroll=False, page_load=False):
            sleep(delay)
            if scroll:
                with suppress(Exception):
                    self.scroll()
            url = self.driver.current_url
            ActionChains(self.driver).double_click(self.element).perform() if double else self.element.click()
            if page_load:
                self.page_load(url)

        def get_attr(self, attr='text', delay=0, scroll=False):
            sleep(delay)
            if scroll:
                with suppress(Exception):
                    self.scroll()
            return getattr(self.element, attr) if attr in ['tag_name', 'text'] else self.element.get_attribute(attr)

        def set_attr(self, value=None, attr='value', delay=0, scroll=False):
            sleep(delay)
            if scroll:
                with suppress(Exception):
                    self.scroll()
            self.driver.execute_script(f"arguments[0].{attr} = arguments[1]", self.element, value)

        def type_keys(self, *value, delay=0, scroll=False, clear=False):
            sleep(delay)
            if scroll:
                with suppress(Exception):
                    self.scroll()
            if clear:
                self.clear()
            self.element.send_keys(*value)

        def select(self, value=None, select_type='select_by_value', delay=0, scroll=False):
            sleep(delay)
            if scroll:
                with suppress(Exception):
                    self.scroll()
            select = Select(self.element)
            function = getattr(select, select_type)
            if value is None:
                if select_type == 'deselect_all':
                    return function()
                else:
                    return select
            else:
                return function(value)

        def find_elements(self, selector, timeout=60, by='xpath'):
            selector = f'.{selector}' if selector[0] != '.' else selector
            if timeout:
                self.wait_element(selector, timeout, by)
            elements = self.element.find_elements(by, selector)
            selector = f'{self.selector}{selector[1:]}'
            elements = [Web.Element(element=element, selector=selector, by=by, driver=self.driver) for element in
                        elements]
            return elements

        def find_element(self, selector, timeout=60, by='xpath'):
            selector = f'.{selector}' if selector[0] != '.' else selector
            if timeout:
                self.wait_element(selector, timeout, by)
            element = self.element.find_element(by, selector)
            selector = f'{self.selector}{selector[1:]}'
            element = Web.Element(element=element, selector=selector, by=by, driver=self.driver)
            return element

        def wait_element(self, selector, timeout=60, by='xpath', until=True):
            selector = f'.{selector}' if selector[0] != '.' else selector

            def find():
                try:
                    self.element.find_element(by, selector)
                    return True
                except (Exception,):
                    return False

            try:
                return wait_until(timeout, 0.5, find, until)
            except (Exception,):
                return False

    def __init__(self, path=None, download_path=None, run=False, timeout=60):
        self.path = path or Path.home().joinpath(r"AppData\Local\.rpa\Chromium\chromedriver.exe")
        self.download_path = download_path or Path.home().joinpath('Downloads')
        self.run_flag = run
        self.timeout = timeout

        self.options = ChromeOptions()
        self.options.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
        self.options.add_experimental_option("useAutomationExtension", False)
        self.options.add_experimental_option("prefs", {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
            "profile.default_content_settings.popups": 0,
            "download.default_directory": self.download_path.__str__(),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False,
            "profile.content_settings.exceptions.automatic_downloads.*.setting": 1
        })
        self.options.add_argument("--start-maximized")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--disable-print-preview")
        self.options.add_argument("--disable-extensions")
        self.options.add_argument("--disable-notifications")
        self.options.add_argument("--ignore-ssl-errors=yes")
        self.options.add_argument("--ignore-certificate-errors")

        # noinspection PyTypeChecker
        self.driver: WebDriver = None

    def run(self):
        self.quit()
        self.driver = webdriver.Chrome(service=Service(self.path.__str__()), options=self.options)

    def quit(self):
        if self.driver:
            self.driver.quit()

    def close(self):
        self.driver.close()

    def switch(self, switch_type='window', switch_index=-1, frame_selector=None):
        if switch_type == 'window':
            self.driver.switch_to.window(self.driver.window_handles[switch_index])
        elif switch_type == 'frame':
            if frame_selector:
                self.driver.switch_to.frame(self.find_elements(frame_selector)[switch_index].element)
            else:
                raise Exception('selected type is "frame", but didnt received frame_selector')
        elif switch_type == 'alert':
            self.driver.switch_to.alert.accept()
        raise Exception(f'switch_type "{switch_type}" didnt found')

    def get(self, url):
        self.driver.get(url)

    def find_elements(self, selector, timeout=None, event=None, by='xpath'):
        if event is None:
            event = expected_conditions.presence_of_element_located
        timeout = timeout if timeout is not None else self.timeout
        if timeout:
            self.wait_element(selector, timeout, event, by)
        elements = self.driver.find_elements(by, selector)
        elements = [self.Element(element=element, selector=selector, by=by, driver=self.driver) for element in elements]
        return elements

    def find_element(self, selector, timeout=None, event=None, by='xpath'):
        if event is None:
            event = expected_conditions.presence_of_element_located
        timeout = timeout if timeout is not None else self.timeout
        if timeout:
            self.wait_element(selector, timeout, event, by)
        element = self.driver.find_element(by, selector)
        element = self.Element(element=element, selector=selector, by=by, driver=self.driver)
        return element

    def wait_element(self, selector, timeout=None, event=None, by='xpath', until=True):
        if event is None:
            event = expected_conditions.presence_of_element_located
        try:
            timeout = timeout if timeout is not None else self.timeout
            wait = WebDriverWait(self.driver, timeout)
            event = event((by, selector))
            wait.until(event) if until else wait.until_not(event)
            return True
        except (Exception,):
            return False

    @staticmethod
    def wait_downloaded(target: Union[Path, str], timeout: Union[int, float] = 60) -> Union[Path, None]:
        start_time = datetime.now()
        while True:
            target = Path(target)
            folder = target.parent
            files = folder.glob(target.name)
            for file_path in files:
                if not any(temp in str(file_path) for temp in ['.crdownload', '~$']):
                    if file_path.is_file() and file_path.stat().st_size > 0:
                        return file_path
            if int((datetime.now() - start_time).seconds) > timeout:
                return None
            sleep(1)

    def refresh(self):
        self.driver.refresh()

    def get_element_display(self, xpath):
        return self.driver.find_element('xpath', xpath).value_of_css_property('display')

    def execute_script(self, xpath, element_type='innerHTML', value=None):

        if element_type == 'innerHTML':
            self.driver.execute_script(f"""
                var xpathExpression = "{xpath}";

                var matchingElements = document.evaluate(xpathExpression, document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);

                for (let i = 0; i < matchingElements.snapshotLength; i++) {{
                  var targetElement = matchingElements.snapshotItem(i);

                  targetElement.innerHTML = "{value}";
                }}
            """)
        elif element_type == 'value':
            self.driver.execute_script(f"""
                    var xpathExpression = "{xpath}";

                    var matchingElements = document.evaluate(xpathExpression, document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);

                    for (let i = 0; i < matchingElements.snapshotLength; i++) {{
                      var targetElement = matchingElements.snapshotItem(i);

                      targetElement.value = "{value}";
                    }}
                """)

    def execute_script_click_js(self, js_path):
        try:
            self.driver.execute_script(f"""
                var button = document.querySelector('{js_path}');
                if (button) {{
                    button.click();
                }}
            """)
        except:
            sleep(10)

    def execute_script_click_xpath(self, xpath):
        try:
            self.driver.execute_script(f"""
                var xpathExpression = "{xpath}";
                var result = document.evaluate(xpathExpression, document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null);
                var element = result.singleNodeValue;

                // Trigger the click event on the element
                if (element) {{
                  element.click();
                }}
            """)
        except:
            sleep(10)
