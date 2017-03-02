(ns coinbase-api.core
  "Implement some trading strategies.

   Ex, buy on dips, sell on spikes. Keep moving buy orders 10% below price 5
   minutes ago. Same for sell order (10% above price 5 minutes ago). If price
   spikes or dips within 5 minutes, orders get executed."
  (:require [gniazdo.core :as ws]
            [clojure.core.async :as a]
            [clj-http.client :as http]
            [clojure.data.json :as json]
            [clj-time.format :as timeformat]
            [clj-time.core :as time]
            [coinbase-api.spec :as sp]
            [clojure.spec :as s])
  (:import [java.util Base64]
           [javax.crypto.spec SecretKeySpec]
           [javax.crypto Mac]
           [java.net URL]
           [java.util UUID]
           [org.apache.commons.math3.distribution LogNormalDistribution])
  (:refer-clojure :exclude [get]))

(defonce current-price (atom nil))
(defonce current-order-book (atom {:sequence 0
                                   :bids (sorted-map-by >)
                                   :asks (sorted-map-by <)}))
(defonce feed-watchers (atom {}))

(def ws-url "wss://ws-feed-public.sandbox.exchange.coinbase.com")
(def ws-url "wss://ws-feed.exchange.coinbase.com")
(def ws-url "wss://ws-feed.gdax.com")
(def api-url "https://api-public.sandbox.exchange.coinbase.com")
(def api-url "https://api.gdax.com")


(defn json-read-str [s]
  (json/read-str s :key-fn keyword))

(defn create-feed-client [ch]
  (println "Creating new feed client")
  (let [conn (ws/connect ws-url :on-receive (fn [v]
                                              (a/put! ch (json-read-str v))))]
    (ws/send-msg conn (json/write-str {:type "subscribe" :product_id "BTC-USD"}))
    (ws/send-msg conn (json/write-str {:type "heartbeat" :on true}))
    conn))

(defn pub-sub [feed-ch]
  (let [m (a/mult feed-ch)
        out1 (a/chan)
        out2 (a/chan)]
    (a/tap m out1)
    (a/tap m out2)
    {:by-type (a/pub out1 :type (fn [_] (a/sliding-buffer 100)))
     :by-order-id-type (a/pub out2 (juxt :order_id :type) (fn [_] (a/sliding-buffer 10)))}))

(defn keep-current-price-updated [match-ch price-atom]
  (a/go-loop []
    (when-let [match (a/<! match-ch)]
      (reset! price-atom (-> match :price read-string))
      (recur))))

(defonce websocket-heartbeat? (atom nil))

(defn keep-feed-running [feed-chan by-type-pub shutdown-ch]
  (let [hb-ch (a/chan (a/sliding-buffer 1))]
    (a/sub by-type-pub "heartbeat" hb-ch)
    (a/go-loop [conn nil]
      (let [[_ ch] (a/alts! [hb-ch (a/timeout 15000) shutdown-ch])]
        (cond (= ch hb-ch)
              (do
                (reset! websocket-heartbeat? true)
                (recur conn))

              (= ch shutdown-ch) nil ;;exit

              :else
              (do ;; feed dead, restart
                (reset! websocket-heartbeat? nil)
                (when conn
                  (try (ws/close conn)
                       (catch Exception _ nil)))
                (recur (try (create-feed-client feed-chan)
                            (catch Exception e
                              (.printStackTrace e))))))))))

(defn init-feed [feed-shutdown-ch]
  (defonce feed-chan (a/chan (a/sliding-buffer 100)))
  (let [pubs (pub-sub feed-chan)
        match-ch (a/chan 1)]
    (a/sub (:by-type pubs) "match" match-ch)
    (keep-current-price-updated match-ch current-price)
    (keep-feed-running feed-chan (:by-type pubs) feed-shutdown-ch)
    pubs))

(defn on-order-filled-watcher
  "Performs an action when receiving 'done' message (reason: filled)
  for a given order id. Fulfills promise p with the feed item value"
  [order-id p]
  (fn [feed-item]
    (when (and (-> feed-item :order_id (= order-id))
               (-> feed-item :type (= "done"))
               (-> feed-item :reason (= "filled")))
      (deliver p feed-item))))

(defn parse-orderbook-entry
  "Convert strings to numbers in order book data"
  [entry]
  (let [[price amount id] entry]
    [(read-string price) (read-string amount) id]))

(defn order-book-insert [order-book feed-item]
  )

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

(declare *credentials*)

(defn get [url]
  (with-coinbase-auth
    (http/get url *credentials*)))

(defn post [url body]
  (with-coinbase-auth
    (http/post url (assoc *credentials* :body body :content-type :json))))

(defn url [path]
  (format "%s%s" api-url path))

(s/fdef limit-order
        :args (s/cat :buy? boolean?
                     :amount :sp/size
                     :price :sp/price)
        :ret :coinbase-api.spec/order)

(defn limit-order "Returns a request for a limit order"
  [buy? amount price]
  {:type "limit"
   :side (if buy? "buy" "sell")
   :price price
   :size amount
   :product_id "BTC-USD"})

(defn accounts []
  (letfn [(parse-account [acct]
            (-> acct
                (update-in [:available] read-string)
                (update-in [:balance] read-string)))]
    (map parse-account (-> "/accounts" url get :body json-read-str))))

(defn balances 
  "Returns a map of currency name to balance - pass either :available or :balance"
  ([k]
   (into {}
         (for [[name accts] (group-by :currency (accounts))]
           [name (-> accts first k)])))
  ([] (balances :balance)))

(defn place-order
  "Places an order and returns the id."
  [order]
  (->> order
       json/write-str
       (post (url "/orders"))
       :body
       json-read-str
       :id))

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

(defn kill-order [order-id]
  (with-coinbase-auth
    (http/delete (url (format "/orders/%s" order-id)) *credentials*)))

(defn kill-all-orders []
  (with-coinbase-auth
    (http/delete (url "/orders") *credentials*)))

(defn orders []
  (with-coinbase-auth
    (-> "/orders" url (http/get *credentials*) :body json-read-str)))

(defn best-orders []
  (-> "/products/BTC-USD/book"
      url
      (http/get {:query-params {:level 1}})
      :body
      json-read-str))

(defn fills []
  (with-coinbase-auth
    (-> "/fills" url (http/get *credentials*) :body json-read-str)))

(defn logistic
  "s-curve calculation for volatility"
  [max steepness midpoint time]
  (/ max (inc (Math/exp (- (* steepness (- time midpoint)))))))

(defn expected-price
  "Given a base price, sigma value for lognormal distribution (lower values
   assign lower probabilities to bigger price moves), an s-curve 'steepness'
   and midpoint, and time-seconds (from now), and cumulative probability p (eg
   p of price = base price is 0.5) - return the expected price."
  [base-price mu sigma p]
  (* base-price (.inverseCumulativeProbability
                 (LogNormalDistribution. (if (> p 0.5)
                                           (- mu)
                                           mu)
                                         sigma) p)))


;; maps probability to fraction of base bet
;; (should sum to 1 ideally)
(def probability-bets
  {0.1 0.1, 0.01 0.30, 0.001 0.60,
   0.9 0.1, 0.99 0.30, 0.999 0.60}
  )
(def narrow-probability-bets
  {0.25 1.0, 0.15 2.0, 0.1 4.0,
   0.75 1.0, 0.85 2.0, 0.9 4.0})


(def base-volatility {60000 [0.002 0.0045] ;; with mu and sigma for lognormal dist
                      (* 10 60000) [0.004 0.012]
                      (* 100 60000) [0.016 0.032]
                      (* 1000 60000) [0.032 0.076]
                      (* 10000 60000) [0.086 0.19]})

(def timescales base-volatility)


(defn round-number
  "Round a double to the given precision (number of significant digits)"
  [factor p]
  (/ (Math/round (* p factor)) factor))

(def round-price (partial round-number 100.0))
(def round-bitcoin (partial round-number 10000000.0))

(defn expire-order
  "When the order is filled, call on-fill-hook with the feed item. If
   order isn't filled by the ttl, kill the order."
  [order-id ttl-ms on-fill-hook match-pub]
  (a/go
    (try (let [fill-ch (a/chan 1)
               topic [order-id "done"]
               sub (a/sub match-pub topic fill-ch)
               [feed-item _] (a/alts! [fill-ch (a/timeout ttl-ms)])]
           (a/unsub match-pub topic fill-ch)
           (if feed-item
             ;; filled, call on-fill-hook
             (do (when (and on-fill-hook
                            (-> feed-item :reason (= "filled")))
                   (on-fill-hook feed-item))
                 feed-item)
             ;; timed out, cancel
             (kill-order order-id)))
        (catch Exception e
          (.printStackTrace e)))))

(defn trade-loop
  "loop continuously, placing buy/sell orders at intervals.

   avail-funds-fraction: Proportion of overall funds to use up placing
   orders (approximate), eg 0.9 = 90%. Using too high a value here might result
   in orders that don't have sufficient funds.

   probability-bets: a reference (var, atom etc) to a mapping of cumulative
   probability to bet-scale. bet-scale is the proportion of funds to use in
   this round of orders compared to other probabilities. eg (atom {0.1 0.4,
   0.01 0.6, 0.9 0.4, 0.99 0.6}) (use 40% of each currency on the 1:10
   probability, and 60% on the 1:100 probability). Any probability over 0.5 is
   a sell, under 0.5 is a buy.

   timescale-params: a ref containing a vector of [ttl mu sigma]. The ttl is
   how long (in ms) the limit order will be in force before it is cancelled (if
   not already filled). mu and sigma are parameters used to calculate the price
   to place orders at, given the current price and probability. see
   expected-price

   overlap-factor: orders stay active for ttl, but if you specify
   overlap-factor > 1, new orders will be placed before old ones
   expire. eg, using ttl of 10 minutes and overlap factor of 5 will
   result in orders being placed every 2 minutes.

   match-pub: a core.async publication to subscribe to order fill
   messages from the exchange. see pub-sub.

   shutdown-ch: a channel, when you close that channel this loop will exit. It
   tries to cancel all outstanding orders before exiting, but this is not
   guaranteed."
  [avail-funds-fraction probability-bets timescale-params
                  overlap-factor match-pub shutdown-ch]
  (a/go-loop []
    (let [[timescale mu sigma] (deref timescale-params)]
      (try (let [balances (balances)]
             (doseq [[probability bet-scale] (deref probability-bets)]
               (let [cur-price @current-price
                     exp-price (expected-price cur-price mu sigma probability)
                     rounded-price (round-price exp-price)
                     buy? (< exp-price cur-price)
                     amount (round-bitcoin (/ (* (if buy?
                                                   (/ (balances "USD") cur-price)
                                                   (balances "BTC"))
                                                 avail-funds-fraction
                                                 bet-scale)
                                              overlap-factor))]
                 (when (and @websocket-heartbeat?
                            (>= amount 0.01)) ;; minimum order
                   (a/<! (a/timeout 200))
                   (let [order-id (place-order (limit-order buy? amount rounded-price))]
                     (expire-order order-id timescale nil match-pub))))))
           (catch Exception e
             (.printStackTrace e)))
      ;; wait before starting next round of orders
      ;; if shutdown channel is closed, kill all orders and exit now
      (let [[_ ch] (a/alts! [shutdown-ch (a/timeout (int (/ timescale overlap-factor)))])]
        (if (= ch shutdown-ch)
          (kill-all-orders)
          (recur))))))

(comment
 (expected-price 890.0 0.016 0.020 0.1)
  (:startup (do

              (def *credentials* {:CB-ACCESS-KEY "7b5fe60c0f3d948984191ca4e32e60e6"
                                  :CB-ACCESS-PASSPHRASE "rju2hl21jwq"
                                  :CB-ACCESS-SECRET "U//grHPDjpr3VVGQCCm6A2ZffDVCT0zdkNNSzgcNe0Wh/HUBfp/jd0Sdk0G+JFJ2LEkMN5JByyGvo7ki0P9Njw=="})
              
              ;; set up channels and pub/sub (should only be once per repl session)
              (defonce feed-chan (a/chan (a/sliding-buffer 100)))
              (defonce pubs (pub-sub feed-chan))

              (do
                (a/close! feed-chan)
                (ns-unmap *ns* 'feed-chan)
                (ns-unmap *ns* 'pubs))
              
              ;; start the websocket feed
              (do (def feed-shutdown-ch (a/chan))
                  (keep-feed-running feed-chan (:by-type pubs) feed-shutdown-ch))

              ;; track current price
              (let [ch (a/chan (a/sliding-buffer 1))]
                (a/sub (:by-type pubs) "match" ch)
                (keep-current-price-updated ch current-price))
              
              ;; stop websocket feed
              (a/close! feed-shutdown-ch)
              
              ;; start trading (example)
              (do
                (def my-probability-bets (atom {0.1 0.05, 0.01 0.35, 0.001 0.60,
                                                0.9 0.05, 0.99 0.35, 0.999 0.60}))
                (def my-timescale (atom [600000 0.004 0.012]))
                (reset! my-timescale [600000 0.016 0.020])
                (def trade-shutdown (a/chan))
                (trade-loop 0.9 my-probability-bets my-timescale 10 (:by-order-id-type pubs) trade-shutdown))

              ;; update trade params
              (reset! my-probability-bets {0.1 0.1, 0.01 0.30, 0.001 0.60,
                                           0.9 0.1, 0.99 0.30, 0.999 0.60})
              (reset! my-timescale [60000 0.016 0.020])
              ;; halt trading
              (a/close! trade-shutdown)

              ;;balances
              (balances)
              
              ;; avg price paid since Jun20
              (let [b (balances)
                    starting-usd-bal 9800.0]
                (/ (- starting-usd-bal (b "USD"))
                   (b "BTC")))

              ;; profit vs buy and holding btc
              (let [b (balances)
                    starting-usd-bal 9800.0
                    starting-btc-price 700
                    total-acct-val-now-if-holding (* starting-usd-bal (/ @current-price starting-btc-price))
                    total-acct-val-now (+ (b "USD") (* (b "BTC") @current-price))]
                (- total-acct-val-now total-acct-val-now-if-holding))
              ;; total account value in USD
              (let [b (balances)] (+ (b "USD") (* (b "BTC") @current-price)))

              ;;last 20 fills
              (map #(select-keys % [:side :size :price :created_at])(take 20 (fills)))
              )


  (expected-price 420 0.18 0.3 16 (* 60 60 24 30) 0.75)


          (map #(expected-price 420 0.23 0.38 14 % 0.01) [60 (* 60 60) (* 60 60 24) (* 60 60 24 7)])

          (map #(expected-price 530 0.002 0.011 %) [0.25 0.1 0.02 0.005 0.001 0.0001 0.00001])  ;; 1m
          (map #(expected-price 530 0.008 0.021 %) [0.25 0.1 0.02 0.005 0.001 0.0001 0.00001])  ;; 10m
          (map #(expected-price 530 0.016 0.052 %) [0.25 0.1 0.02 0.005 0.001 0.0001 0.00001])  ;; 100m
          (map #(expected-price 530 0.032 0.076 %) [0.25 0.1 0.02 0.005 0.001 0.0001 0.00001])  ;; 1000m 
          (map #(expected-price 530 0.086 0.19 %) [0.25 0.1 0.02 0.005 0.001])  ;; 10000m
          (make-trade 420 0.001 60)

          (defn make-trades [current-price c-ts]
            (for [[cum-prob time] c-ts]
              [(> 0.5 cum-prob) (make-trade current-price cum-prob time) ]))

          ;; 1 min 60s - ~1
          ;; 5 min 300s - ~1.2 1.2
          ;; 1 hour 3600s - ~2 (/ 2 1.2) 1.6666666666666667
          ;; 6 hour 21600s - ~9 (/ 9 2.0) 4.5
          ;; 24 hour 86400s - ~14 (/ 14 9.0) 1.5555555555555556
          ;; weekly 608400s - ~27 (/ 27 14.0) 1.9285714285714286
          (* 3600 24 7)
          (+ 1 (/ (Math/pow 608400 0.9) 1000))

          (defn time-fn [s]
            (Math/pow (inc (Math/log s)) 2))
          (map time-fn [60 300 3600 21600 86400 608400])
          (def foo (partial logistic 1 0.68 15))
          (for [[a b] (partition 2 1
                                 (map (comp #(+ 0.03 %) foo #(Math/log %)) [60 300 3600 21600 86400 608400])
                                 )]
            (/ b a))
          (defn n-day-vol [b h]
            ()) (Math/log 608400)

          (Math/log 7200)

          (map #(Math/log %) [60 300 3600 21600 86400 608400])))
