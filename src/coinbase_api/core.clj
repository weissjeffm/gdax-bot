(ns coinbase-api.core
  "Implement some trading strategies.

   Ex, buy on dips, sell on spikes. Keep moving buy orders 10% below price 5
   minutes ago. Same for sell order (10% above price 5 minutes ago). If price
   spikes or dips within 5 minutes, orders get executed."
  (:require [gniazdo.core :as ws]
            [clj-http.client :as http]
            [clojure.data.json :as json]
            [clj-time.format :as timeformat]
            [clj-time.core :as time])
  (:import [java.util Base64]
           [javax.crypto.spec SecretKeySpec]
           [javax.crypto Mac]
           [java.net URL]
           [java.util UUID])
  (:refer-clojure :exclude [get]))

(defn create-feed-client [endpoint f]
  (let [conn (ws/connect endpoint :on-receive f)]
    (ws/send-msg conn (json/write-str {:type "subscribe" :product_id "BTC-USD"}))
    conn))

(def ws-url "wss://ws-feed-public.sandbox.exchange.coinbase.com")
(def ws-url "wss://ws-feed.exchange.coinbase.com")
(def api-url "https://api-public.sandbox.exchange.coinbase.com")
(def api-url "https://api.exchange.coinbase.com")

(defn parse-orderbook-entry
  "Convert strings to numbers in order book data"
  [entry]
  (let [[price amount id] entry]
    [(read-string price) (read-string amount) id]))

(defn json-read-str [s]
  (json/read-str s :key-fn keyword))

(defn order-book
  "Get the order book from the exchange"
  []
  (-> (http/get (format "%s%s" api-url "/products/BTC-USD/book")
                {:query-params {:level 3}})
      :body
      json-read-str
      (update-in [:bids] (partial map parse-orderbook-entry))
      (update-in [:asks] (partial map parse-orderbook-entry))))

(defn cost
  "How much it would cost/generate to buy/sell a given amount at market
   price,given the order-book. k is either :bids (to sell) or :asks (to buy)"
  [order-book k amount]
  (let [side (k order-book)
        sort-fn (if (= k :bids) reverse identity)
        sorted-book (->> side (sort-by first) sort-fn)]
    (loop [total-cost 0
           remaining amount
           entries side]
      (if (empty? entries)
        (throw (IllegalArgumentException. "Order book not deep enough to fulfill order")))
      (let [[price amount _] (first entries)
            end-of-order (>= amount remaining)]
        (if end-of-order
          (+ total-cost (* price remaining))
          (recur (+ total-cost (* price amount))
                 (- remaining amount)
                 (rest entries)))))))

(defn trades-page
  "Returns paginated trades as a vector of [trades next-page-id]"
  ([page-id]
   (let [response (-> (http/get (format "%s%s" api-url "/products/BTC-USD/trades")
                                (when page-id {:query-params {"after" page-id}})))
         trades (-> response
                    :body
                    json-read-str)
         next-page-id (-> response :headers (clojure.core/get "cb-before"))]
     [trades next-page-id])))

(def date-format (timeformat/formatter "yyyy-MM-dd HH:mm:ss.SSSSSSZZ"))

(def format-date (partial timeformat/parse date-format))

(defn parse-trade [trade]
  (-> trade
      (update-in [:price] read-string)
      (update-in [:size] read-string)))

(defn filter-trades
  "Returns all trades newer than sec-in-past"
  [trades sec-in-past]
  (let [cutoff (time/from-now (time/seconds (- sec-in-past)))]
    (take-while (fn [trade]
                  (-> trade :time format-date (time/after? cutoff)))
                trades)))

(defn all-trades-since
  "all trades since n seconds ago"
  ([n next-id]
   (filter-trades
    (let [[trades next-id] (trades-page next-id)
          trades (map parse-trade trades)]
      (lazy-seq (concat trades (all-trades-since n next-id))))
    n))
  ([n]
   (all-trades-since n nil)))

(defn price-difference
  "Price change between n seconds ago and now"
  [n]
  (let [trades (all-trades-since n)
        oldest (last trades)
        newest (first trades)]
    (- (:price newest) (:price oldest))))


(defonce hmac (javax.crypto.Mac/getInstance "HmacSHA256"))

(defn encode [bs]
  (-> (Base64/getEncoder) (.encodeToString bs)))

(defn decode [s]
  (-> (Base64/getDecoder) (.decode s)))

(defn sign [key message]
  (.init hmac (SecretKeySpec. key "HmacSHA256"))
  (.doFinal hmac message))

(defn wrap-coinbase-auth [client]
  (fn [req]
    (let [sk (-> req :CB-ACCESS-SECRET decode)
          timestamp (format "%f" (/ (System/currentTimeMillis) 1000.0))
          sign-message (str timestamp
                            (-> req :method name .toUpperCase)
                            (-> req :url (URL.) .getPath)
                            (:body req))
          headers {:CB-ACCESS-KEY (:CB-ACCESS-KEY req)
                   :CB-ACCESS-SIGN (->> sign-message .getBytes (sign sk) encode)
                   :CB-ACCESS-TIMESTAMP timestamp
                   :CB-ACCESS-PASSPHRASE (:CB-ACCESS-PASSPHRASE req)}]
      (client (update-in req [:headers] merge headers)))))

(defmacro with-coinbase-auth [& body]
  `(http/with-middleware (conj http/default-middleware #'wrap-coinbase-auth)
     ~@body))

(def ^:dynamic *credentials* nil)

(defn get [url]
  (with-coinbase-auth
    (http/get url *credentials*)))

(defn post [url body]
  (with-coinbase-auth
    (http/post url (assoc *credentials* :body body :content-type :json))))

(defn url [path]
  (format "%s%s" api-url path))

(defn limit-order "Returns a request for a limit order"
  [side price]
  {:side side :price price :product_id "BTC-USD"})

(defn accounts []
  (letfn [(parse-account [acct]
            (-> acct
                (update-in [:available] read-string)
                (update-in [:balance] read-string)))]
    (map parse-account (-> "/accounts" url get :body json-read-str))))

(defn place-order [order]
  (->> order
       json/write-str
       (post (url "/orders"))
       :body
       json-read-str))

(defn sell-bitcoin [amount price]
  (place-order {:type "limit"
                :side "sell"
                :price price
                :size amount
                :product_id "BTC-USD"}))

(defn buy-bitcoin [amount price]
  (place-order {:type "limit"
                :side "buy"
                :price price
                :size amount
                :product_id "BTC-USD"}))

(defn kill-all-orders []
  (with-coinbase-auth
    (http/delete (url "/orders") *credentials*)))

(defn orders []
  (with-coinbase-auth
    (http/get (url "/orders") *credentials*)))

(defn best-orders []
  (-> "/products/BTC-USD/book"
      url
      (http/get {:query-params {:level 1}})
      :body
      json-read-str))

(defn fills []
  (with-coinbase-auth
    (-> "/fills" url (http/get *credentials*) :body json-read-str)))
