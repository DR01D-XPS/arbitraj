import json
import threading
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import ccxt
import requests
import tkinter as tk
from tkinter import ttk


EXCHANGES: List[Tuple[str, str]] = [
    ("binance", "Binance"),
    ("bybit", "Bybit"),
    ("coinbase", "Coinbase"),
    ("okx", "OKX"),
    ("kraken", "Kraken"),
    ("gateio", "Gate.io"),
    ("mexc", "MEXC"),
    ("bitget", "Bitget"),
    ("htx", "HTX"),
    ("upbit", "Upbit"),
    ("kucoin", "KuCoin"),
    ("bingx", "BingX"),
    ("cryptocom", "Crypto.com"),
    ("bitmart", "BitMart"),
    ("lbank", "LBank"),
    ("whitebit", "WhiteBIT"),
    ("poloniex", "Poloniex"),
    ("bitstamp", "Bitstamp"),
    ("coinex", "CoinEx"),
    ("btse", "BTSE"),
    ("bitfinex", "Bitfinex"),
]

FALLBACK_QUOTES = ["USDT", "USD", "USDC", "BTC"]
SETTINGS_FILE = "user_settings.json"
BLACKLIST_FILE = "coin_blacklist.json"
POPULAR_START_COUNT = 500
LONG_SCAN_LIMIT = 10000
SAVED_TOP_LIMIT = 10
SAVED_TOP_POOL_LIMIT = 15
SAVED_BATCH_ADD = 5
NETWORK_ALIASES = {
    "ERC20": "ETHEREUM",
    "ETH": "ETHEREUM",
    "ARBITRUMONE": "ARBITRUM",
    "ARBONE": "ARBITRUM",
    "ARBEVM": "ARBITRUM",
    "BEP20": "BSC",
    "BSC": "BSC",
    "BSC(BEP20)": "BSC",
    "TRC20": "TRON",
    "TRX": "TRON",
    "MATIC": "POLYGON",
    "POLYGON": "POLYGON",
    "SOL": "SOLANA",
    "AVAXC": "AVALANCHE-C",
    "AVAXC-CHAIN": "AVALANCHE-C",
    "OPTIMISM": "OPTIMISM",
    "OP": "OPTIMISM",
}
class SavedTopWindow:
    def __init__(
        self,
        app: "PriceTrackerApp",
        title: str,
        coins: List[str],
        exchanges: List[str],
    ) -> None:
        self.app = app
        self.coins = list(coins)
        self.exchanges = list(exchanges)
        self.window = tk.Toplevel(app.root)
        self.window.title(title)
        self.window.geometry("1500x820")
        self.window.minsize(640, 360)
        self.window.resizable(True, True)
        self.window.configure(bg="#0f131a")
        self.alive = True

        top = ttk.Frame(self.window, padding=10)
        top.pack(fill=tk.X)
        self.title_label = tk.Label(
            top,
            text=title,
            bg="#0f131a",
            fg="#8fb4ff",
            font=("Consolas", 14, "bold"),
        )
        self.title_label.pack(anchor=tk.W)
        self.coins_label = tk.Label(
            top,
            text=f"Монеты: {', '.join(self.coins)}",
            bg="#0f131a",
            fg="#b7c4dd",
            font=("Consolas", 10),
        )
        self.coins_label.pack(anchor=tk.W, pady=(4, 0))

        table_wrap = tk.Frame(self.window, bg="#111827", bd=1, relief=tk.FLAT)
        table_wrap.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        self.canvas = tk.Canvas(table_wrap, bg="#111827", highlightthickness=0)
        self.scroll_y = ttk.Scrollbar(table_wrap, orient=tk.VERTICAL, command=self.canvas.yview)
        self.scroll_x = ttk.Scrollbar(table_wrap, orient=tk.HORIZONTAL, command=self.canvas.xview)
        self.canvas.configure(yscrollcommand=self.scroll_y.set, xscrollcommand=self.scroll_x.set)
        self.scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        self.scroll_x.pack(side=tk.BOTTOM, fill=tk.X)
        self.canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.inner = tk.Frame(self.canvas, bg="#111827")
        self.inner.bind("<Configure>", lambda _: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas.create_window((0, 0), window=self.inner, anchor="nw")

        self.status_var = tk.StringVar(value="Ожидание обновления...")
        ttk.Label(self.window, textvariable=self.status_var).pack(anchor=tk.W, padx=10, pady=(0, 10))

        self.window.protocol("WM_DELETE_WINDOW", self._on_close)

    def _on_close(self) -> None:
        self.alive = False
        self.window.destroy()

    def sync_with_main(self, coins: List[str], exchanges: List[str], title: str) -> None:
        self.coins = list(coins)
        self.exchanges = list(exchanges)
        self.window.title(title)
        self.title_label.configure(text=title)
        self.coins_label.configure(text=f"Монеты: {', '.join(self.coins)}")

    def render(self, items: List[Tuple[str, Dict[str, object]]]) -> None:
        if not self.alive:
            return

        for child in self.inner.winfo_children():
            child.destroy()

        headers = ["X", "MONETA", "PAIR", "TX"] + [self.app.exchange_name_by_id[ex_id] for ex_id in self.exchanges] + ["% RAZNICA"]
        for col, header in enumerate(headers):
            tk.Label(
                self.inner,
                text=header,
                bg="#1e293b",
                fg="#9ab6ff",
                font=("Consolas", 10, "bold"),
                padx=6,
                pady=6,
                relief=tk.GROOVE,
                borderwidth=1,
            ).grid(row=0, column=col, sticky="nsew")

        for row_idx, (coin, row_data) in enumerate(items, start=1):
            spread = row_data.get("spread")
            min_ex = row_data.get("min_ex")
            max_ex = row_data.get("max_ex")
            coin_bg = "#111827" if row_idx % 2 else "#0f172a"

            remove_btn = tk.Label(
                self.inner,
                text="x",
                bg=coin_bg,
                fg="#ff8c8c",
                font=("Consolas", 10, "bold"),
                padx=6,
                pady=5,
                relief=tk.GROOVE,
                borderwidth=1,
                cursor="hand2",
            )
            remove_btn.grid(row=row_idx, column=0, sticky="nsew")
            remove_btn.bind("<Button-1>", lambda _e, c=coin: self.app.exclude_saved_top_coin(c))

            tk.Label(
                self.inner,
                text=coin,
                bg=coin_bg,
                fg="#d7dde8",
                font=("Consolas", 10, "bold"),
                padx=6,
                pady=5,
                relief=tk.GROOVE,
                borderwidth=1,
            ).grid(row=row_idx, column=1, sticky="nsew")

            pair_label = tk.Label(
                self.inner,
                text=str(row_data.get("pair", "-")),
                bg=coin_bg,
                fg="#bac7dd",
                font=("Consolas", 10),
                padx=6,
                pady=5,
                relief=tk.GROOVE,
                borderwidth=1,
                cursor="hand2" if min_ex and max_ex else "",
            )
            pair_label.grid(row=row_idx, column=2, sticky="nsew")
            if min_ex and max_ex:
                pair_label.bind(
                    "<Button-1>",
                    lambda _e, row=row_data: self.app.open_pair_links(row),
                )

            tx_text = str(row_data.get("tx", "NO"))
            tk.Label(
                self.inner,
                text=tx_text,
                bg=coin_bg,
                fg="#8dd6ff" if tx_text in {"GOOO", "YES"} else "#8fa1bf",
                font=("Consolas", 9, "bold"),
                padx=4,
                pady=5,
                relief=tk.GROOVE,
                borderwidth=1,
            ).grid(row=row_idx, column=3, sticky="nsew")

            for c_off, exchange_id in enumerate(self.exchanges, start=4):
                price = row_data["prices"].get(exchange_id)
                link = row_data["links"].get(exchange_id)
                bg = coin_bg
                fg = "#d7dde8"
                if exchange_id == min_ex:
                    bg = "#0f3d26"
                    fg = "#9cffc7"
                if exchange_id == max_ex:
                    bg = "#4c1d1d"
                    fg = "#ffb3b3"

                cell = tk.Label(
                    self.inner,
                    text=self.app._format_price(price),
                    bg=bg,
                    fg=fg,
                    font=("Consolas", 10, "underline" if link and price is not None else "normal"),
                    cursor="hand2" if link and price is not None else "",
                    padx=6,
                    pady=5,
                    relief=tk.GROOVE,
                    borderwidth=1,
                )
                cell.grid(row=row_idx, column=c_off, sticky="nsew")
                if link and price is not None:
                    cell.bind("<Button-1>", lambda _e, url=link: webbrowser.open_new_tab(url))

            spread_text = "N/A" if spread is None else f"{spread:.2f}%"
            spread_fg = "#8fa1bf" if spread is None else "#ffe08a"
            tk.Label(
                self.inner,
                text=spread_text,
                bg=coin_bg,
                fg=spread_fg,
                font=("Consolas", 10, "bold"),
                padx=6,
                pady=5,
                relief=tk.GROOVE,
                borderwidth=1,
            ).grid(row=row_idx, column=len(headers) - 1, sticky="nsew")

        for col in range(len(headers)):
            self.inner.grid_columnconfigure(col, weight=1)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.status_var.set(f"Обновлено: {now} | Монет: {len(items)}")


class PriceTrackerApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Crypto Arbitrage IDE")
        self.root.geometry("1700x960")
        self.root.minsize(1300, 760)
        self.root.configure(bg="#0f131a")

        self.exchange_clients: Dict[str, ccxt.Exchange] = {}
        self.exchange_markets: Dict[str, set] = {}
        self.exchange_locks: Dict[str, threading.Lock] = {}
        self.exchange_market_locks: Dict[str, threading.Lock] = {}
        self.exchange_available: Dict[str, bool] = {}
        self.exchange_currency_networks: Dict[str, Dict[str, List[dict]]] = {}

        self.auto_refresh_job: Optional[str] = None
        self.is_loading_exchanges = False
        self.is_refreshing = False

        self.exchange_name_by_id = {exchange_id: name for exchange_id, name in EXCHANGES}
        self.exchange_order = [exchange_id for exchange_id, _ in EXCHANGES]
        self.exchange_vars: Dict[str, tk.BooleanVar] = {}
        self.bybit_universe: List[str] = []
        self.bybit_cursor = 0
        self.bybit_cycle_count = 0
        self.bybit_universe_ready = False
        self.scan_batch_size = 50
        self.saved_top_window: Optional[SavedTopWindow] = None
        self.saved_top_memory: Dict[str, Dict[str, object]] = {}
        self.saved_top_excluded: set[str] = set()
        self.blacklist: set[str] = set()

        self._load_blacklist(silent=True)
        self._build_ui()
        self.load_settings(silent=True)
        self._bootstrap_exchanges_async()

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self) -> None:
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("TFrame", background="#0f131a")
        style.configure("TLabel", background="#0f131a", foreground="#d7dde8")
        style.configure("TButton", background="#202938", foreground="#e6edf8")
        style.map("TButton", background=[("active", "#2a364b")])
        style.configure("TEntry", fieldbackground="#111827", foreground="#dbe6ff")
        style.configure(
            "Dark.TCombobox",
            fieldbackground="#111827",
            background="#111827",
            foreground="#e6edf8",
            arrowcolor="#d7dde8",
        )
        style.map(
            "Dark.TCombobox",
            fieldbackground=[("readonly", "#111827"), ("!disabled", "#111827")],
            background=[("readonly", "#111827"), ("!disabled", "#111827")],
            foreground=[("readonly", "#e6edf8"), ("!disabled", "#e6edf8")],
            selectbackground=[("readonly", "#111827"), ("!disabled", "#111827")],
            selectforeground=[("readonly", "#e6edf8"), ("!disabled", "#e6edf8")],
        )
        style.configure("Vertical.TScrollbar", background="#1f2937")
        style.configure("Horizontal.TScrollbar", background="#1f2937")

        top = ttk.Frame(self.root, padding=10)
        top.pack(fill=tk.X)

        self.title_label = tk.Label(
            top,
            text="CRYPTO ARBITRAGE IDE",
            bg="#0f131a",
            fg="#8fb4ff",
            font=("Consolas", 16, "bold"),
        )
        self.title_label.pack(anchor=tk.W)

        controls = ttk.Frame(top)
        controls.pack(fill=tk.X, pady=(8, 0))

        ttk.Label(controls, text="Монеты (через запятую):").pack(side=tk.LEFT)
        self.coins_entry = ttk.Entry(controls, width=62)
        self.coins_entry.pack(side=tk.LEFT, padx=(8, 14))

        ttk.Label(controls, text="Режим:").pack(side=tk.LEFT)
        self.scan_mode_var = tk.StringVar(value="AUTO")
        self.scan_mode_combo = ttk.Combobox(
            controls,
            textvariable=self.scan_mode_var,
            values=["AUTO", "MANUAL"],
            width=8,
            state="readonly",
            style="Dark.TCombobox",
        )
        self.scan_mode_combo.pack(side=tk.LEFT, padx=(8, 14))

        ttk.Label(controls, text="Котировка:").pack(side=tk.LEFT)
        self.quote_var = tk.StringVar(value="USDT")
        self.quote_combo = ttk.Combobox(
            controls,
            textvariable=self.quote_var,
            values=["USDT", "USD", "USDC", "BTC", "ETH"],
            width=8,
            state="readonly",
            style="Dark.TCombobox",
        )
        self.quote_combo.pack(side=tk.LEFT, padx=(8, 14))

        ttk.Label(controls, text="Интервал (сек):").pack(side=tk.LEFT)
        self.interval_var = tk.StringVar(value="20")
        self.interval_entry = ttk.Entry(controls, textvariable=self.interval_var, width=6)
        self.interval_entry.pack(side=tk.LEFT, padx=(8, 14))

        self.refresh_btn = ttk.Button(controls, text="Обновить", command=self.refresh_prices_async)
        self.refresh_btn.pack(side=tk.LEFT)

        self.auto_btn = ttk.Button(controls, text="Авто ВКЛ", command=self.start_auto_refresh)
        self.auto_btn.pack(side=tk.LEFT, padx=(8, 0))

        self.stop_btn = ttk.Button(controls, text="Авто ВЫКЛ", command=self.stop_auto_refresh)
        self.stop_btn.pack(side=tk.LEFT, padx=(8, 0))

        self.save_btn = ttk.Button(controls, text="Сохранить", command=self.save_settings)
        self.save_btn.pack(side=tk.LEFT, padx=(8, 0))

        self.load_btn = ttk.Button(controls, text="Загрузить", command=self.load_settings)
        self.load_btn.pack(side=tk.LEFT, padx=(8, 0))

        filters = ttk.Frame(top)
        filters.pack(fill=tk.X, pady=(10, 0))

        self.sort_by_spread_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(filters, text="Сортировать по % разницы (убыв.)", variable=self.sort_by_spread_var).pack(side=tk.LEFT)

        self.verified_only_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(filters, text="Только проверенные (YES/GOOO)", variable=self.verified_only_var).pack(side=tk.LEFT, padx=(14, 0))

        self.good_volume_only_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(filters, text="Хороший объём", variable=self.good_volume_only_var).pack(side=tk.LEFT, padx=(14, 0))

        ttk.Label(filters, text="Мин. объём (тыс.$):").pack(side=tk.LEFT, padx=(14, 0))
        self.min_volume_k_var = tk.StringVar(value="1")
        self.min_volume_k_entry = ttk.Entry(filters, textvariable=self.min_volume_k_var, width=7)
        self.min_volume_k_entry.pack(side=tk.LEFT, padx=(8, 14))

        ttk.Label(filters, text="Мин. % разницы:").pack(side=tk.LEFT, padx=(14, 0))
        self.min_spread_var = tk.StringVar(value="0")
        self.min_spread_entry = ttk.Entry(filters, textvariable=self.min_spread_var, width=7)
        self.min_spread_entry.pack(side=tk.LEFT, padx=(8, 14))

        ttk.Label(filters, text="Показать TOP:").pack(side=tk.LEFT)
        self.top_n_var = tk.StringVar(value="50")
        self.top_n_combo = ttk.Combobox(
            filters,
            textvariable=self.top_n_var,
            values=["ALL", "10", "20", "50", "100"],
            width=6,
            state="readonly",
            style="Dark.TCombobox",
        )
        self.top_n_combo.pack(side=tk.LEFT, padx=(8, 0))

        blacklist_bar = ttk.Frame(top)
        blacklist_bar.pack(fill=tk.X, pady=(10, 0))

        ttk.Label(blacklist_bar, text="Blacklist:").pack(side=tk.LEFT)
        self.blacklist_entry = ttk.Entry(blacklist_bar, width=42)
        self.blacklist_entry.pack(side=tk.LEFT, padx=(8, 8))
        ttk.Button(blacklist_bar, text="Добавить", command=self.add_blacklist_from_entry).pack(side=tk.LEFT)
        ttk.Button(blacklist_bar, text="Убрать", command=self.remove_blacklist_from_entry).pack(side=tk.LEFT, padx=(8, 0))
        self.blacklist_label = ttk.Label(blacklist_bar, text=self._blacklist_label_text())
        self.blacklist_label.pack(side=tk.LEFT, padx=(14, 0))

        body = ttk.Panedwindow(self.root, orient=tk.HORIZONTAL)
        body.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))

        sidebar = ttk.Frame(body, width=290, padding=8)
        body.add(sidebar, weight=0)

        table_area = ttk.Frame(body, padding=(8, 8, 8, 0))
        body.add(table_area, weight=1)

        self._build_exchange_sidebar(sidebar)
        self._build_table_area(table_area)
        self._build_log_area(table_area)

        status_bar = ttk.Frame(self.root, padding=(10, 0, 10, 10))
        status_bar.pack(fill=tk.X)
        self.status_var = tk.StringVar(value="Инициализация...")
        ttk.Label(status_bar, textvariable=self.status_var).pack(anchor=tk.W)

    def _build_exchange_sidebar(self, parent: ttk.Frame) -> None:
        panel = tk.Frame(parent, bg="#111827", bd=1, relief=tk.FLAT)
        panel.pack(fill=tk.BOTH, expand=True)

        tk.Label(
            panel,
            text="БИРЖИ",
            bg="#111827",
            fg="#98b5ff",
            font=("Consolas", 13, "bold"),
            anchor="w",
        ).pack(fill=tk.X, padx=10, pady=(10, 2))

        tk.Label(
            panel,
            text="Добавляй/убирай биржи для сравнения",
            bg="#111827",
            fg="#8b98b0",
            font=("Consolas", 9),
            anchor="w",
        ).pack(fill=tk.X, padx=10, pady=(0, 8))

        btns = tk.Frame(panel, bg="#111827")
        btns.pack(fill=tk.X, padx=10, pady=(0, 8))

        tk.Button(
            btns,
            text="Выбрать все",
            command=self._select_all_exchanges,
            bg="#1f2937",
            fg="#d7dde8",
            activebackground="#334155",
            relief=tk.FLAT,
        ).pack(side=tk.LEFT, padx=(0, 6))

        tk.Button(
            btns,
            text="Снять все",
            command=self._clear_all_exchanges,
            bg="#1f2937",
            fg="#d7dde8",
            activebackground="#334155",
            relief=tk.FLAT,
        ).pack(side=tk.LEFT)

        wrap = tk.Frame(panel, bg="#111827")
        wrap.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        self.ex_canvas = tk.Canvas(wrap, bg="#111827", highlightthickness=0)
        self.ex_scroll = ttk.Scrollbar(wrap, orient=tk.VERTICAL, command=self.ex_canvas.yview)
        self.ex_canvas.configure(yscrollcommand=self.ex_scroll.set)

        self.ex_inner = tk.Frame(self.ex_canvas, bg="#111827")
        self.ex_inner.bind("<Configure>", lambda _: self.ex_canvas.configure(scrollregion=self.ex_canvas.bbox("all")))

        self.ex_canvas.create_window((0, 0), window=self.ex_inner, anchor="nw")
        self.ex_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.ex_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        for exchange_id, exchange_name in EXCHANGES:
            var = tk.BooleanVar(value=exchange_id in {"binance", "bybit", "okx", "kucoin", "bingx", "coinbase", "kraken", "mexc", "bitget", "gateio"})
            self.exchange_vars[exchange_id] = var
            cb = tk.Checkbutton(
                self.ex_inner,
                text=exchange_name,
                variable=var,
                bg="#111827",
                fg="#d7dde8",
                selectcolor="#0b1220",
                activebackground="#111827",
                activeforeground="#d7dde8",
                anchor="w",
                command=self._on_exchange_selection_changed,
            )
            cb.pack(fill=tk.X, padx=4, pady=1)

    def _build_table_area(self, parent: ttk.Frame) -> None:
        frame = tk.Frame(parent, bg="#111827", bd=1, relief=tk.FLAT)
        frame.pack(fill=tk.BOTH, expand=True)

        self.table_canvas = tk.Canvas(frame, bg="#111827", highlightthickness=0)
        self.table_scroll_y = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=self.table_canvas.yview)
        self.table_scroll_x = ttk.Scrollbar(frame, orient=tk.HORIZONTAL, command=self.table_canvas.xview)
        self.table_canvas.configure(yscrollcommand=self.table_scroll_y.set, xscrollcommand=self.table_scroll_x.set)

        self.table_scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        self.table_scroll_x.pack(side=tk.BOTTOM, fill=tk.X)
        self.table_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.table_inner = tk.Frame(self.table_canvas, bg="#111827")
        self.table_inner.bind("<Configure>", lambda _: self.table_canvas.configure(scrollregion=self.table_canvas.bbox("all")))
        self.table_canvas.create_window((0, 0), window=self.table_inner, anchor="nw")

    def _build_log_area(self, parent: ttk.Frame) -> None:
        log_wrap = tk.Frame(parent, bg="#111827", bd=1, relief=tk.FLAT)
        log_wrap.pack(fill=tk.BOTH, expand=False, pady=(8, 0))

        tk.Label(
            log_wrap,
            text="LOG",
            bg="#111827",
            fg="#98b5ff",
            font=("Consolas", 11, "bold"),
            anchor="w",
        ).pack(fill=tk.X, padx=8, pady=(6, 4))

        self.log_text = tk.Text(
            log_wrap,
            height=8,
            wrap=tk.WORD,
            bg="#0b1220",
            fg="#b7c4dd",
            insertbackground="#d7dde8",
            relief=tk.FLAT,
            font=("Consolas", 10),
            state=tk.DISABLED,
        )
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

    def log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def _settings_payload(self) -> dict:
        return {
            "coins": self.coins_entry.get().strip(),
            "scan_mode": self.scan_mode_var.get().strip().upper(),
            "quote": self.quote_var.get().strip(),
            "interval": self.interval_var.get().strip(),
            "sort_by_spread": bool(self.sort_by_spread_var.get()),
            "verified_only": bool(self.verified_only_var.get()),
            "good_volume_only": bool(self.good_volume_only_var.get()),
            "min_volume_k": self.min_volume_k_var.get().strip(),
            "min_spread": self.min_spread_var.get().strip(),
            "top_n": self.top_n_var.get().strip(),
            "selected_exchanges": self._selected_exchange_ids(),
            "geometry": self.root.geometry(),
        }

    def save_settings(self, silent: bool = False) -> bool:
        try:
            with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
                json.dump(self._settings_payload(), f, ensure_ascii=False, indent=2)
            if not silent:
                self.log(f"Настройки сохранены в {SETTINGS_FILE}.")
            return True
        except Exception as exc:
            if not silent:
                self.log(f"Ошибка сохранения настроек: {exc}")
            return False

    def load_settings(self, silent: bool = False) -> bool:
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            if not silent:
                self.log("Файл настроек не найден.")
            return False
        except Exception as exc:
            if not silent:
                self.log(f"Ошибка загрузки настроек: {exc}")
            return False

        coins = str(data.get("coins", "")).strip()
        if coins:
            self.coins_entry.delete(0, tk.END)
            self.coins_entry.insert(0, coins)

        scan_mode = str(data.get("scan_mode", "AUTO")).strip().upper()
        if scan_mode in {"AUTO", "MANUAL"}:
            self.scan_mode_var.set(scan_mode)

        quote = str(data.get("quote", "")).strip().upper()
        if quote in {"USDT", "USD", "USDC", "BTC", "ETH"}:
            self.quote_var.set(quote)

        interval = str(data.get("interval", "")).strip()
        if interval:
            self.interval_var.set(interval)

        self.sort_by_spread_var.set(bool(data.get("sort_by_spread", True)))
        self.verified_only_var.set(bool(data.get("verified_only", False)))
        self.good_volume_only_var.set(bool(data.get("good_volume_only", False)))

        min_volume_k = str(data.get("min_volume_k", "")).strip()
        if min_volume_k:
            self.min_volume_k_var.set(min_volume_k)

        min_spread = str(data.get("min_spread", "")).strip()
        if min_spread:
            self.min_spread_var.set(min_spread)

        top_n = str(data.get("top_n", "")).strip().upper()
        if top_n in {"ALL", "10", "20", "50", "100"}:
            self.top_n_var.set(top_n)

        selected = data.get("selected_exchanges")
        if isinstance(selected, list):
            selected_set = {str(x) for x in selected}
            for ex_id, var in self.exchange_vars.items():
                var.set(ex_id in selected_set)

        geometry = str(data.get("geometry", "")).strip()
        if geometry and "x" in geometry:
            try:
                self.root.geometry(geometry)
            except Exception:
                pass

        self._on_exchange_selection_changed()
        if not silent:
            self.log(f"Настройки загружены из {SETTINGS_FILE}.")
            self.refresh_prices_async()
        return bool(coins)

    def _on_close(self) -> None:
        self.save_settings(silent=True)
        self._save_blacklist(silent=True)
        if self.saved_top_window and self.saved_top_window.alive:
            self.saved_top_window._on_close()
        self.root.destroy()

    def _load_blacklist(self, silent: bool = False) -> None:
        try:
            with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                self.blacklist = {
                    str(item).strip().upper() for item in data if str(item).strip()
                }
        except FileNotFoundError:
            self.blacklist = set()
        except Exception as exc:
            self.blacklist = set()
            if not silent:
                self.log(f"Ошибка загрузки blacklist: {exc}")

    def _save_blacklist(self, silent: bool = False) -> bool:
        try:
            with open(BLACKLIST_FILE, "w", encoding="utf-8") as f:
                json.dump(sorted(self.blacklist), f, ensure_ascii=False, indent=2)
            if not silent:
                self.log(f"Blacklist сохранен в {BLACKLIST_FILE}.")
            return True
        except Exception as exc:
            if not silent:
                self.log(f"Ошибка сохранения blacklist: {exc}")
            return False

    def _blacklist_label_text(self) -> str:
        return f"В blacklist: {len(self.blacklist)}"

    def _refresh_blacklist_label(self) -> None:
        if hasattr(self, "blacklist_label"):
            self.blacklist_label.configure(text=self._blacklist_label_text())

    def _parse_coin_list(self, value: str, skip_blacklist: bool = True) -> List[str]:
        seen = set()
        coins: List[str] = []
        for raw in value.split(","):
            coin = raw.strip().upper()
            if not coin or coin in seen:
                continue
            if skip_blacklist and coin in self.blacklist:
                continue
            seen.add(coin)
            coins.append(coin)
        return coins

    def add_blacklist_from_entry(self) -> None:
        coins = self._parse_coin_list(self.blacklist_entry.get())
        if not coins:
            self.log("Blacklist: нет монет для добавления.")
            return
        for coin in coins:
            self.blacklist.add(coin)
            self.saved_top_memory.pop(coin, None)
            self.saved_top_excluded.add(coin)
        self._save_blacklist(silent=True)
        self._refresh_blacklist_label()
        self.blacklist_entry.delete(0, tk.END)
        if self.saved_top_window and self.saved_top_window.alive:
            self._render_saved_top_window(self.saved_top_window.exchanges)
        self.log(f"Добавлено в blacklist: {', '.join(coins)}.")

    def remove_blacklist_from_entry(self) -> None:
        coins = self._parse_coin_list(self.blacklist_entry.get(), skip_blacklist=False)
        if not coins:
            self.log("Blacklist: нет монет для удаления.")
            return
        removed = [coin for coin in coins if coin in self.blacklist]
        for coin in removed:
            self.blacklist.discard(coin)
        self._save_blacklist(silent=True)
        self._refresh_blacklist_label()
        self.blacklist_entry.delete(0, tk.END)
        self.log(f"Удалено из blacklist: {', '.join(removed) if removed else '-'}")

    def _bootstrap_exchanges_async(self) -> None:
        if self.is_loading_exchanges:
            return

        self.is_loading_exchanges = True
        self.status_var.set("Инициализация бирж...")
        self.log("Запуск инициализации подключений к биржам.")

        def worker() -> None:
            ok = 0
            for exchange_id, exchange_name in EXCHANGES:
                try:
                    client_cls = getattr(ccxt, exchange_id)
                    client = client_cls({"enableRateLimit": True, "timeout": 15000})
                    self.exchange_clients[exchange_id] = client
                    self.exchange_markets[exchange_id] = set()
                    self.exchange_locks[exchange_id] = threading.Lock()
                    self.exchange_market_locks[exchange_id] = threading.Lock()
                    self.exchange_available[exchange_id] = True
                    self.exchange_currency_networks[exchange_id] = {}
                    ok += 1
                    self.root.after(0, lambda n=exchange_name: self.log(f"{n}: API клиент готов."))
                except Exception as exc:
                    self.exchange_available[exchange_id] = False
                    self.root.after(0, lambda n=exchange_name, e=exc: self.log(f"{n}: недоступна ({e})."))

            self.root.after(0, lambda: self._on_bootstrap_complete(ok))

        threading.Thread(target=worker, daemon=True).start()

    def _on_bootstrap_complete(self, ok: int) -> None:
        self.is_loading_exchanges = False
        self.status_var.set(f"Готово. Клиентов бирж: {ok}/{len(EXCHANGES)}")
        self.log(f"Инициализация завершена: {ok}/{len(EXCHANGES)}.")
        self._load_bybit_universe_async()

    def _load_bybit_universe_async(self) -> None:
        self.log("Загрузка 500 популярных монет и длинного universe...")

        def worker() -> None:
            symbols = self._fetch_bybit_universe()
            self.root.after(0, lambda: self._apply_bybit_universe(symbols))

        threading.Thread(target=worker, daemon=True).start()

    def _fetch_bybit_universe(self) -> List[str]:
        def worker(exchange_id: str) -> List[str]:
            if not self._ensure_exchange_markets(exchange_id):
                return []
            client = self.exchange_clients.get(exchange_id)
            if client is None:
                return []
            markets = getattr(client, "markets", {}) or {}
            coins = set()
            for _symbol, meta in markets.items():
                if not bool(meta.get("spot")):
                    continue
                base = str(meta.get("base", "")).upper().strip()
                if base:
                    coins.add(base)
            return sorted(coins)

        coins = set()
        exchange_ids = [exchange_id for exchange_id, _ in EXCHANGES]
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(worker, ex_id): ex_id for ex_id in exchange_ids}
            for future in as_completed(futures):
                try:
                    coins.update(future.result())
                except Exception as exc:
                    ex_id = futures[future]
                    self.root.after(0, lambda e=exc, x=ex_id: self.log(f"Ошибка universe {x}: {e}"))

        global_universe = [coin for coin in sorted(coins) if coin not in self.blacklist][:LONG_SCAN_LIMIT]
        popular = self._fetch_popular_symbols(global_universe)
        popular_set = set(popular)
        tail = [coin for coin in global_universe if coin not in popular_set]
        return popular + tail

    def _fetch_popular_symbols(self, available_coins: List[str]) -> List[str]:
        available_set = set(available_coins)
        popular: List[str] = []
        seen = set()
        try:
            for page in [1, 2]:
                response = requests.get(
                    "https://api.coingecko.com/api/v3/coins/markets",
                    params={
                        "vs_currency": "usd",
                        "order": "market_cap_desc",
                        "per_page": 250,
                        "page": page,
                        "sparkline": "false",
                    },
                    timeout=12,
                )
                response.raise_for_status()
                for item in response.json():
                    symbol = str(item.get("symbol", "")).upper().strip()
                    if symbol and symbol in available_set and symbol not in seen:
                        seen.add(symbol)
                        popular.append(symbol)
                if len(popular) >= POPULAR_START_COUNT:
                    break
        except Exception as exc:
            self.root.after(0, lambda e=exc: self.log(f"Не удалось загрузить топ-500 популярных монет: {e}"))

        if len(popular) < POPULAR_START_COUNT:
            fallback = available_coins[:POPULAR_START_COUNT]
            for coin in fallback:
                if coin not in seen:
                    popular.append(coin)
        return popular[:POPULAR_START_COUNT]

    def _apply_bybit_universe(self, symbols: List[str]) -> None:
        if not symbols:
            self.bybit_universe_ready = False
            self.status_var.set("Не удалось загрузить глобальный список монет.")
            return

        self.bybit_universe = symbols
        self.bybit_cursor = 0
        self.bybit_cycle_count = 0
        self.bybit_universe_ready = True
        self.log(f"Universe готов: сначала {POPULAR_START_COUNT} популярных, затем длинный хвост. Всего {len(symbols)} монет.")
        if self.scan_mode_var.get().strip().upper() == "AUTO":
            self._take_next_bybit_batch()
        self.refresh_prices_async()

    def _take_next_bybit_batch(self) -> List[str]:
        if not self.bybit_universe:
            return []

        batch: List[str] = []
        target_size = min(self.scan_batch_size, len(self.bybit_universe))
        attempts = 0
        max_attempts = len(self.bybit_universe) * 2
        while len(batch) < target_size and attempts < max_attempts:
            coin = self.bybit_universe[self.bybit_cursor]
            self.bybit_cursor = (self.bybit_cursor + 1) % len(self.bybit_universe)
            if self.bybit_cursor == 0:
                self.bybit_cycle_count += 1
                self.log(f"Завершен цикл сканирования #{self.bybit_cycle_count}.")
            attempts += 1
            if coin in self.blacklist or coin in batch:
                continue
            batch.append(coin)

        if self.scan_mode_var.get().strip().upper() == "AUTO":
            self.coins_entry.delete(0, tk.END)
            self.coins_entry.insert(0, ", ".join(batch))
        return batch

    def _selected_exchange_ids(self) -> List[str]:
        return [ex_id for ex_id in self.exchange_order if self.exchange_vars[ex_id].get()]

    def _select_all_exchanges(self) -> None:
        for var in self.exchange_vars.values():
            var.set(True)
        self._on_exchange_selection_changed()

    def _clear_all_exchanges(self) -> None:
        for var in self.exchange_vars.values():
            var.set(False)
        self._on_exchange_selection_changed()

    def _on_exchange_selection_changed(self) -> None:
        selected = self._selected_exchange_ids()
        self.status_var.set(f"Выбрано бирж: {len(selected)}")

    def _format_price(self, price: Optional[float]) -> str:
        if price is None:
            return "N/A"
        if price >= 1000:
            return f"{price:,.2f}"
        if price >= 1:
            return f"{price:,.4f}"
        return f"{price:,.8f}"

    def _normalize_network(self, value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        cleaned = "".join(ch for ch in str(value).upper() if ch.isalnum())
        if not cleaned:
            return None
        return NETWORK_ALIASES.get(cleaned, cleaned)

    def _build_exchange_metadata_index(self, exchange_id: str) -> None:
        client = self.exchange_clients.get(exchange_id)
        if client is None:
            return

        currencies = getattr(client, "currencies", {}) or {}
        currency_networks: Dict[str, List[dict]] = {}

        for code, currency in currencies.items():
            base_code = str(code).upper().strip()
            if not base_code:
                continue

            parsed_networks: List[dict] = []
            networks = currency.get("networks") or {}
            for network_name, network in networks.items():
                info = network.get("info") or {}
                normalized_network = self._normalize_network(
                    network.get("network") or network_name or info.get("chain") or info.get("name")
                )
                parsed = {
                    "network": normalized_network,
                    "display": network.get("network") or network_name,
                    "deposit": network.get("deposit"),
                    "withdraw": network.get("withdraw"),
                    "active": network.get("active"),
                }
                parsed_networks.append(parsed)

            if parsed_networks:
                currency_networks[base_code] = parsed_networks

        self.exchange_currency_networks[exchange_id] = currency_networks

    def _build_symbol_candidates(self, coin: str, preferred_quote: str) -> List[str]:
        quotes = [preferred_quote] + [q for q in FALLBACK_QUOTES if q != preferred_quote]
        return [f"{coin}/{q}" for q in quotes]

    def _ensure_exchange_markets(self, exchange_id: str) -> bool:
        if not self.exchange_available.get(exchange_id, False):
            return False

        if self.exchange_markets.get(exchange_id):
            return True

        lock = self.exchange_market_locks.get(exchange_id)
        client = self.exchange_clients.get(exchange_id)
        if lock is None or client is None:
            return False

        with lock:
            if self.exchange_markets.get(exchange_id):
                return True
            try:
                markets = client.load_markets()
                self.exchange_markets[exchange_id] = set(markets.keys())
                self._build_exchange_metadata_index(exchange_id)
                return True
            except Exception:
                self.exchange_available[exchange_id] = False
                return False

    def _extract_price(self, ticker: Optional[dict]) -> Optional[float]:
        if not ticker:
            return None
        value = ticker.get("last") or ticker.get("close") or ticker.get("bid")
        try:
            return float(value) if value else None
        except (TypeError, ValueError):
            return None

    def _extract_float(self, value: object) -> Optional[float]:
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _is_usd_quote(self, quote: str) -> bool:
        return quote.upper() in {"USD", "USDT", "USDC", "FDUSD", "TUSD", "USDE", "DAI"}

    def _quote_to_usd_multiplier(
        self,
        exchange_id: str,
        quote: str,
        tickers_map: Dict[str, dict],
        client: ccxt.Exchange,
        lock: threading.Lock,
    ) -> Optional[float]:
        quote_upper = quote.upper()
        if self._is_usd_quote(quote_upper):
            return 1.0

        for symbol in [f"{quote_upper}/USDT", f"{quote_upper}/USD", f"{quote_upper}/USDC"]:
            ticker = tickers_map.get(symbol)
            if ticker is None:
                if symbol not in self.exchange_markets.get(exchange_id, set()):
                    continue
                try:
                    with lock:
                        ticker = client.fetch_ticker(symbol)
                    tickers_map[symbol] = ticker
                except Exception:
                    continue

            price = self._extract_price(ticker)
            if price is not None and price > 0:
                return price
        return None

    def _extract_volume_usd(
        self,
        exchange_id: str,
        symbol: str,
        ticker: Optional[dict],
        price: Optional[float],
        tickers_map: Dict[str, dict],
        client: ccxt.Exchange,
        lock: threading.Lock,
    ) -> Optional[float]:
        if not ticker or not symbol:
            return None

        try:
            _base, quote = symbol.split("/")
        except ValueError:
            return None

        quote_volume = self._extract_float(ticker.get("quoteVolume"))
        if quote_volume is None:
            base_volume = self._extract_float(ticker.get("baseVolume"))
            if base_volume is not None and price is not None:
                quote_volume = base_volume * price
        if quote_volume is None or quote_volume <= 0:
            return None

        multiplier = self._quote_to_usd_multiplier(exchange_id, quote, tickers_map, client, lock)
        if multiplier is None or multiplier <= 0:
            return None
        return quote_volume * multiplier

    def _resolve_symbol_candidates(
        self,
        exchange_id: str,
        coin: str,
        preferred_quote: str,
    ) -> List[Tuple[str, str]]:
        markets = self.exchange_markets.get(exchange_id, set())
        base_code = coin.strip().upper()
        resolved: List[Tuple[str, str]] = []
        for candidate in self._build_symbol_candidates(base_code, preferred_quote):
            if candidate in markets:
                resolved.append((base_code, candidate))
        return resolved

    def _asset_meta_for_symbol(self, exchange_id: str, base_code: str, symbol: str) -> dict:
        networks = list(self.exchange_currency_networks.get(exchange_id, {}).get(base_code.upper(), []))
        return {
            "base_code": base_code.upper(),
            "networks": networks,
        }

    def _fetch_prices_for_exchange(
        self,
        exchange_id: str,
        coins: List[str],
        preferred_quote: str,
    ) -> Tuple[str, Dict[str, Tuple[Optional[float], str, Optional[str], dict, Optional[float]]]]:
        result: Dict[str, Tuple[Optional[float], str, Optional[str], dict, Optional[float]]] = {
            coin: (None, "-", None, {}, None) for coin in coins
        }
        if not self._ensure_exchange_markets(exchange_id):
            return exchange_id, result

        client = self.exchange_clients.get(exchange_id)
        lock = self.exchange_locks.get(exchange_id)
        markets = self.exchange_markets.get(exchange_id, set())
        if client is None or lock is None:
            return exchange_id, result

        symbol_by_coin: Dict[str, Tuple[str, str]] = {}
        symbols: List[str] = []
        for coin in coins:
            candidates = self._resolve_symbol_candidates(
                exchange_id,
                coin,
                preferred_quote,
            )
            if candidates:
                base_code, symbol = candidates[0]
                symbol_by_coin[coin] = (base_code, symbol)
                symbols.append(symbol)

        if not symbols:
            return exchange_id, result

        tickers_map: Dict[str, dict] = {}
        has_fetch_tickers = bool(client.has.get("fetchTickers")) if hasattr(client, "has") else False
        missing_symbols = list(symbols)

        if has_fetch_tickers:
            try:
                with lock:
                    batch = client.fetch_tickers(symbols)
                if isinstance(batch, dict):
                    tickers_map = batch
                    missing_symbols = [s for s in symbols if s not in tickers_map]
            except Exception:
                missing_symbols = list(symbols)

        if missing_symbols:
            for symbol in missing_symbols:
                try:
                    with lock:
                        tickers_map[symbol] = client.fetch_ticker(symbol)
                except Exception:
                    continue

        for coin, (base_code, symbol) in symbol_by_coin.items():
            ticker = tickers_map.get(symbol)
            price = self._extract_price(ticker)
            link = self._build_exchange_link(exchange_id, symbol)
            meta = self._asset_meta_for_symbol(exchange_id, base_code, symbol)
            volume_usd = self._extract_volume_usd(exchange_id, symbol, ticker, price, tickers_map, client, lock)
            result[coin] = (price, symbol, link, meta, volume_usd)

        return exchange_id, result

    def _build_exchange_link(self, exchange_id: str, symbol: str) -> Optional[str]:
        try:
            base, quote = symbol.split("/")
        except ValueError:
            return None

        base_u, quote_u = base.upper(), quote.upper()
        base_l, quote_l = base.lower(), quote.lower()

        templates = {
            "binance": f"https://www.binance.com/en/trade/{base_u}_{quote_u}",
            "bybit": f"https://www.bybit.com/trade/spot/{base_u}/{quote_u}",
            "coinbase": f"https://www.coinbase.com/advanced-trade/spot/{base_u}-{quote_u}",
            "okx": f"https://www.okx.com/trade-spot/{base_l}-{quote_l}",
            "kraken": f"https://pro.kraken.com/app/trade/{base_l}-{quote_l}",
            "gateio": f"https://www.gate.io/trade/{base_u}_{quote_u}",
            "mexc": f"https://www.mexc.com/exchange/{base_u}_{quote_u}",
            "bitget": f"https://www.bitget.com/spot/{base_u}{quote_u}",
            "htx": f"https://www.htx.com/trade/{base_l}_{quote_l}",
            "upbit": f"https://upbit.com/exchange?code=CRIX.UPBIT.{quote_u}-{base_u}",
            "kucoin": f"https://www.kucoin.com/trade/{base_u}-{quote_u}",
            "bingx": f"https://bingx.com/en/spot/{base_u}{quote_u}/",
            "cryptocom": f"https://crypto.com/exchange/trade/spot/{base_u}_{quote_u}",
            "bitmart": f"https://www.bitmart.com/trade/en-US?symbol={base_u}_{quote_u}",
            "lbank": f"https://www.lbank.com/trade/{base_l}_{quote_l}/",
            "whitebit": f"https://whitebit.com/trade/{base_u}-{quote_u}",
            "poloniex": f"https://poloniex.com/trade/{base_u}_{quote_u}/?type=spot",
            "bitstamp": f"https://www.bitstamp.net/trade/{base_l}/{quote_l}/",
            "coinex": f"https://www.coinex.com/exchange/{base_l}-{quote_l}",
            "btse": f"https://www.btse.com/en/trading/{base_u}-{quote_u}",
            "bitfinex": f"https://trading.bitfinex.com/t/{base_u}:{quote_u}?type=exchange",
        }
        return templates.get(exchange_id)

    def open_pair_links(self, row_data: Dict[str, object]) -> None:
        min_ex = row_data.get("min_ex")
        max_ex = row_data.get("max_ex")
        links = row_data.get("links", {})
        if not isinstance(links, dict):
            return

        for ex_id in [min_ex, max_ex]:
            if isinstance(ex_id, str):
                url = links.get(ex_id)
                if url:
                    webbrowser.open_new_tab(url)

    def exclude_saved_top_coin(self, coin: str) -> None:
        normalized = coin.strip().upper()
        self.saved_top_excluded.add(normalized)
        if normalized in self.saved_top_memory:
            self.saved_top_memory.pop(normalized, None)
        self.log(f"Монета {normalized} исключена из сохраненного топа до конца сеанса.")
        if self.saved_top_window and self.saved_top_window.alive:
            self._render_saved_top_window(self.saved_top_window.exchanges)

    def _collect_rows_for_coins(
        self,
        coins: List[str],
        selected_exchanges: List[str],
        preferred_quote: str,
    ) -> Dict[str, Dict[str, object]]:
        rows: Dict[str, Dict[str, object]] = {
            coin: {
                "pair": "-",
                "prices": {exchange_id: None for exchange_id in selected_exchanges},
                "symbols": {exchange_id: "-" for exchange_id in selected_exchanges},
                "links": {exchange_id: None for exchange_id in selected_exchanges},
                "volumes": {exchange_id: None for exchange_id in selected_exchanges},
                "asset_meta": {exchange_id: {} for exchange_id in selected_exchanges},
                "spread": None,
                "min_ex": None,
                "max_ex": None,
                "route": "N/A",
                "tx": "NO",
                "min_volume_usd": None,
                "max_volume_usd": None,
            }
            for coin in coins
        }

        tasks = []
        max_workers = min(24, max(1, len(selected_exchanges)))
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            for exchange_id in selected_exchanges:
                if exchange_id not in self.exchange_clients:
                    continue
                tasks.append(
                    pool.submit(
                        self._fetch_prices_for_exchange,
                        exchange_id,
                        coins,
                        preferred_quote,
                    )
                )

            for future in as_completed(tasks):
                exchange_id, exchange_rows = future.result()
                for coin, (price, symbol, link, meta, volume_usd) in exchange_rows.items():
                    row = rows[coin]
                    row["prices"][exchange_id] = price
                    row["symbols"][exchange_id] = symbol
                    row["links"][exchange_id] = link
                    row["volumes"][exchange_id] = volume_usd
                    row["asset_meta"][exchange_id] = meta
                    if symbol != "-" and row["pair"] == "-":
                        row["pair"] = symbol

        for coin in coins:
            row = rows[coin]
            valid_prices = [
                (ex_id, row["prices"][ex_id])
                for ex_id in selected_exchanges
                if isinstance(row["prices"][ex_id], float)
            ]
            if len(valid_prices) < 2:
                continue

            min_ex, min_price = min(valid_prices, key=lambda x: x[1])
            max_ex, max_price = max(valid_prices, key=lambda x: x[1])
            if max_price <= min_price:
                continue

            route = self._find_transfer_route(
                row["asset_meta"].get(min_ex, {}),
                row["asset_meta"].get(max_ex, {}),
            )
            if route is None:
                continue

            spread = ((max_price - min_price) / min_price * 100.0) if min_price > 0 else None
            if spread is None:
                continue

            row["min_ex"] = min_ex
            row["max_ex"] = max_ex
            row["spread"] = spread
            row["route"] = route
            row["tx"] = "GOOO" if route != "UNVERIFIED" else "YES"
            row["min_volume_usd"] = row["volumes"].get(min_ex)
            row["max_volume_usd"] = row["volumes"].get(max_ex)

        return rows

    def _find_transfer_route(self, source_meta: dict, target_meta: dict) -> Optional[str]:
        source_code = str(source_meta.get("base_code", "")).upper().strip()
        target_code = str(target_meta.get("base_code", "")).upper().strip()
        if not source_code or source_code != target_code:
            return None

        source_networks = source_meta.get("networks") or []
        target_networks = target_meta.get("networks") or []
        if source_networks and target_networks:
            for src in source_networks:
                if src.get("withdraw") is False or src.get("active") is False:
                    continue
                src_key = src.get("network")
                if not src_key:
                    continue
                for dst in target_networks:
                    if dst.get("deposit") is False or dst.get("active") is False:
                        continue
                    if dst.get("network") != src_key:
                        continue
                    return str(src.get("display") or src_key)

        if source_code == target_code and not source_networks and not target_networks:
            return "UNVERIFIED"

        return None

    def _update_saved_top_from_items(
        self,
        items: List[Tuple[str, Dict[str, object]]],
        exchanges: List[str],
    ) -> None:
        additions = [(coin, row) for coin, row in items if coin not in self.saved_top_excluded][:SAVED_BATCH_ADD]
        if not additions:
            self.root.after(0, lambda: self.log("В текущем batch нет валидных монет для сохраненного топа."))
            return

        added_now = 0
        for coin, row in additions:
            if coin in self.blacklist:
                continue
            existing = self.saved_top_memory.get(coin)
            existing_spread = existing.get("spread") if isinstance(existing, dict) else None
            new_spread = row.get("spread")
            if existing is None:
                self.saved_top_memory[coin] = row
                added_now += 1
            elif (
                isinstance(new_spread, float)
                and (not isinstance(existing_spread, float) or new_spread > existing_spread)
            ):
                self.saved_top_memory[coin] = row

        top15 = sorted(
            self.saved_top_memory.items(),
            key=lambda x: x[1].get("spread") if x[1].get("spread") is not None else -1,
            reverse=True,
        )[:SAVED_TOP_POOL_LIMIT]
        self.saved_top_memory = {coin: row for coin, row in top15}

        self.root.after(0, lambda: self._render_saved_top_window(exchanges))
        self.root.after(
            0,
            lambda n=added_now, total=len(self.saved_top_memory): self.log(
                f"Сохраненный топ обновлен: +{n} новых, показано {min(total, SAVED_TOP_LIMIT)}/{SAVED_TOP_LIMIT}, резерв {max(total - SAVED_TOP_LIMIT, 0)}/{SAVED_TOP_POOL_LIMIT - SAVED_TOP_LIMIT}."
            ),
        )

    def _saved_top_items(self) -> List[Tuple[str, Dict[str, object]]]:
        return sorted(
            [(coin, row) for coin, row in self.saved_top_memory.items() if coin not in self.saved_top_excluded],
            key=lambda x: x[1].get("spread") if x[1].get("spread") is not None else -1,
            reverse=True,
        )[:SAVED_TOP_LIMIT]

    def _saved_top_pool_items(self) -> List[Tuple[str, Dict[str, object]]]:
        return sorted(
            [(coin, row) for coin, row in self.saved_top_memory.items() if coin not in self.saved_top_excluded],
            key=lambda x: x[1].get("spread") if x[1].get("spread") is not None else -1,
            reverse=True,
        )[:SAVED_TOP_POOL_LIMIT]

    def _render_saved_top_window(self, exchanges: List[str]) -> None:
        if not self.saved_top_memory:
            return

        items = self._saved_top_items()
        coins = [coin for coin, _ in items]
        title = "Сохраненный TOP (10 лучших + 5 резерв)"

        if self.saved_top_window is None or not self.saved_top_window.alive:
            self.saved_top_window = SavedTopWindow(
                app=self,
                title=title,
                coins=coins,
                exchanges=exchanges,
            )
        else:
            self.saved_top_window.sync_with_main(coins, exchanges, title)

        self.saved_top_window.render(items)

    def _refresh_saved_window_async(
        self,
        selected_exchanges: List[str],
        preferred_quote: str,
    ) -> None:
        if self.saved_top_window is None or not self.saved_top_window.alive:
            return
        if not self.saved_top_memory:
            return

        coins = [coin for coin, _ in self._saved_top_pool_items()]

        def worker() -> None:
            rows = self._collect_rows_for_coins(
                coins,
                selected_exchanges,
                preferred_quote,
            )
            for coin, row in rows.items():
                if coin in self.saved_top_memory and row.get("spread") is not None:
                    self.saved_top_memory[coin] = row
            stale = [
                coin for coin, row in self.saved_top_memory.items()
                if coin in coins and row.get("spread") is None
            ]
            for coin in stale:
                self.saved_top_memory.pop(coin, None)
            self.root.after(0, lambda: self._render_saved_top_window(selected_exchanges))

        threading.Thread(target=worker, daemon=True).start()

    def refresh_prices_async(self) -> None:
        if self.is_loading_exchanges:
            self.log("Идет инициализация бирж, дождитесь завершения.")
            return
        if self.is_refreshing:
            self.log("Обновление уже в процессе.")
            return
        mode = self.scan_mode_var.get().strip().upper()
        if mode == "AUTO" and not self.bybit_universe_ready:
            self.status_var.set("Ожидаю глобальный список монет...")
            self.log("Глобальный universe еще не готов.")
            return

        if mode == "MANUAL":
            coins = self._parse_coin_list(self.coins_entry.get().strip())
        else:
            coins = self._take_next_bybit_batch()
        if not coins:
            self.status_var.set("Список монет пуст.")
            self.log("Не удалось получить batch монет.")
            return

        selected_exchanges = self._selected_exchange_ids()
        if not selected_exchanges:
            self.status_var.set("Выберите минимум одну биржу.")
            self.log("Не выбраны биржи.")
            return

        min_spread = 0.0
        try:
            min_spread = float(self.min_spread_var.get().strip() or "0")
        except ValueError:
            min_spread = 0.0

        sort_by_spread = bool(self.sort_by_spread_var.get())
        verified_only = bool(self.verified_only_var.get())
        good_volume_only = bool(self.good_volume_only_var.get())
        top_n_raw = self.top_n_var.get().strip().upper()

        min_volume_usd = 1000.0
        try:
            min_volume_usd = max(0.0, float(self.min_volume_k_var.get().strip() or "1") * 1000.0)
        except ValueError:
            min_volume_usd = 1000.0

        self.is_refreshing = True
        self.refresh_btn.configure(state=tk.DISABLED)
        self.status_var.set("Обновление данных...")
        self.log(f"Обновление ({mode}): скан batch={len(coins)}, бирж={len(selected_exchanges)}.")

        preferred_quote = self.quote_var.get().strip().upper() or "USDT"

        def worker() -> None:
            rows = self._collect_rows_for_coins(
                coins,
                selected_exchanges,
                preferred_quote,
            )
            filtered = self._apply_filters(
                rows,
                coins,
                min_spread,
                sort_by_spread,
                top_n_raw,
                verified_only,
                good_volume_only,
                min_volume_usd,
            )
            self._update_saved_top_from_items(filtered, selected_exchanges)
            self.root.after(0, lambda: self._render_table(filtered, selected_exchanges))
            self._refresh_saved_window_async(
                selected_exchanges,
                preferred_quote,
            )

        threading.Thread(target=worker, daemon=True).start()

    def _apply_filters(
        self,
        rows: Dict[str, Dict[str, object]],
        coins: List[str],
        min_spread: float,
        sort_by_spread: bool,
        top_n_raw: str,
        verified_only: bool,
        good_volume_only: bool,
        min_volume_usd: float,
    ) -> List[Tuple[str, Dict[str, object]]]:
        items: List[Tuple[str, Dict[str, object]]] = []
        for coin in coins:
            if coin in self.blacklist:
                continue
            row = rows[coin]
            spread = row.get("spread")
            if spread is None:
                continue
            if not isinstance(spread, float) or spread > 99:
                continue
            if spread < min_spread:
                continue
            if verified_only and row.get("tx") not in {"YES", "GOOO"}:
                continue
            if good_volume_only:
                buy_volume = row.get("min_volume_usd")
                sell_volume = row.get("max_volume_usd")
                if not isinstance(buy_volume, float) or not isinstance(sell_volume, float):
                    continue
                if buy_volume < min_volume_usd or sell_volume < min_volume_usd:
                    continue
            items.append((coin, row))

        if sort_by_spread:
            items.sort(key=lambda x: x[1].get("spread") if x[1].get("spread") is not None else -1, reverse=True)

        if top_n_raw != "ALL":
            try:
                n = int(top_n_raw)
                if n > 0:
                    items = items[:n]
            except ValueError:
                pass

        return items

    def _render_table(self, items: List[Tuple[str, Dict[str, object]]], selected_exchanges: List[str]) -> None:
        for child in self.table_inner.winfo_children():
            child.destroy()

        headers = ["MONETA", "PAIR", "TX"] + [self.exchange_name_by_id[ex_id] for ex_id in selected_exchanges] + ["% RAZNICA"]

        widths = [110, 130, 60] + [125 for _ in selected_exchanges] + [120]
        for col, header in enumerate(headers):
            lbl = tk.Label(
                self.table_inner,
                text=header,
                bg="#1e293b",
                fg="#9ab6ff",
                font=("Consolas", 10, "bold"),
                padx=6,
                pady=6,
                relief=tk.GROOVE,
                borderwidth=1,
                width=max(8, widths[col] // 10),
            )
            lbl.grid(row=0, column=col, sticky="nsew")

        for row_idx, (coin, row_data) in enumerate(items, start=1):
            spread = row_data.get("spread")
            min_ex = row_data.get("min_ex")
            max_ex = row_data.get("max_ex")

            coin_bg = "#111827" if row_idx % 2 else "#0f172a"

            tk.Label(
                self.table_inner,
                text=coin,
                bg=coin_bg,
                fg="#d7dde8",
                font=("Consolas", 10, "bold"),
                padx=6,
                pady=5,
                relief=tk.GROOVE,
                borderwidth=1,
            ).grid(row=row_idx, column=0, sticky="nsew")

            pair_label = tk.Label(
                self.table_inner,
                text=str(row_data.get("pair", "-")),
                bg=coin_bg,
                fg="#bac7dd",
                font=("Consolas", 10),
                padx=6,
                pady=5,
                relief=tk.GROOVE,
                borderwidth=1,
                cursor="hand2" if row_data.get("min_ex") and row_data.get("max_ex") else "",
            )
            pair_label.grid(row=row_idx, column=1, sticky="nsew")
            if row_data.get("min_ex") and row_data.get("max_ex"):
                pair_label.bind("<Button-1>", lambda _e, row=row_data: self.open_pair_links(row))

            tx_text = str(row_data.get("tx", "NO"))
            tk.Label(
                self.table_inner,
                text=tx_text,
                bg=coin_bg,
                fg="#8dd6ff" if tx_text in {"GOOO", "YES"} else "#8fa1bf",
                font=("Consolas", 9, "bold"),
                padx=4,
                pady=5,
                relief=tk.GROOVE,
                borderwidth=1,
            ).grid(row=row_idx, column=2, sticky="nsew")

            for c_off, exchange_id in enumerate(selected_exchanges, start=3):
                price = row_data["prices"].get(exchange_id)
                link = row_data["links"].get(exchange_id)

                bg = coin_bg
                fg = "#d7dde8"
                if exchange_id == min_ex:
                    bg = "#0f3d26"
                    fg = "#9cffc7"
                if exchange_id == max_ex:
                    bg = "#4c1d1d"
                    fg = "#ffb3b3"

                label = tk.Label(
                    self.table_inner,
                    text=self._format_price(price),
                    bg=bg,
                    fg=fg,
                    font=("Consolas", 10, "underline" if link and price is not None else "normal"),
                    cursor="hand2" if link and price is not None else "",
                    padx=6,
                    pady=5,
                    relief=tk.GROOVE,
                    borderwidth=1,
                )
                label.grid(row=row_idx, column=c_off, sticky="nsew")

                if link and price is not None:
                    label.bind("<Button-1>", lambda _e, url=link: webbrowser.open_new_tab(url))

            spread_text = "N/A" if spread is None else f"{spread:.2f}%"
            spread_fg = "#8fa1bf" if spread is None else "#ffe08a"
            tk.Label(
                self.table_inner,
                text=spread_text,
                bg=coin_bg,
                fg=spread_fg,
                font=("Consolas", 10, "bold"),
                padx=6,
                pady=5,
                relief=tk.GROOVE,
                borderwidth=1,
            ).grid(row=row_idx, column=len(headers) - 1, sticky="nsew")

        for col in range(len(headers)):
            self.table_inner.grid_columnconfigure(col, weight=1)

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.status_var.set(f"Обновлено: {now} | Строк: {len(items)}")
        self.log("Таблица цен обновлена.")

        self.is_refreshing = False
        self.refresh_btn.configure(state=tk.NORMAL)

    def start_auto_refresh(self) -> None:
        interval = self._get_interval_seconds()
        if interval is None:
            return
        self.stop_auto_refresh()
        self.log(f"Автообновление включено ({interval} сек).")
        self._schedule_auto_refresh(interval)

    def _schedule_auto_refresh(self, interval: int) -> None:
        self.refresh_prices_async()
        self.auto_refresh_job = self.root.after(interval * 1000, lambda: self._schedule_auto_refresh(interval))

    def stop_auto_refresh(self) -> None:
        if self.auto_refresh_job:
            self.root.after_cancel(self.auto_refresh_job)
            self.auto_refresh_job = None
            self.log("Автообновление выключено.")

    def _get_interval_seconds(self) -> Optional[int]:
        try:
            val = int(self.interval_var.get().strip())
            if val < 5:
                raise ValueError
            return val
        except ValueError:
            self.status_var.set("Интервал: целое число >= 5")
            self.log("Ошибка: некорректный интервал.")
            return None


if __name__ == "__main__":
    root = tk.Tk()
    app = PriceTrackerApp(root)
    root.mainloop()

