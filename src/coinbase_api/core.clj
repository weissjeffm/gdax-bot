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
            [clj-time.core :as time])
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
(def api-url "https://api-public.sandbox.exchange.coinbase.com")
(def api-url "https://api.exchange.coinbase.com")

(defn json-read-str [s]
  (json/read-str s :key-fn keyword))

(defn create-feed-client [endpoint]
  (let [ch (a/chan (a/sliding-buffer 100))
        conn (ws/connect endpoint :on-receive (fn [v]
                                                (a/put! ch (json-read-str v))))]
    (ws/send-msg conn (json/write-str {:type "subscribe" :product_id "BTC-USD"}))
    {:conn conn
     :out ch}))

(defn pub-sub [feed-ch]
  (let [m (a/mult feed-ch)
        out1 (a/chan)
        out2 (a/chan)]
    (a/tap m out1)
    (a/tap m out2)
    {:by-type (a/pub out1 :type (constantly 100))
     :by-order-id-type (a/pub out2 (juxt :order_id :type))}))

(defn keep-current-price-updated [match-ch price-atom]
  (a/go-loop []
    (when-let [match (a/<! match-ch)]
      (reset! price-atom (-> match :price read-string))
      (recur))))

(defn init-feed []
  (let [{:keys [conn out]} (create-feed-client ws-url)
        pubs (pub-sub out)
        match-ch (a/chan 1)]
    (a/sub (:by-type pubs) "match" match-ch)
    (keep-current-price-updated match-ch current-price)
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

(defn available-balance "Where currency is \"BTC\", \"USD\" etc"
  [currency]
  (->> (accounts)
       (filter #(= (:currency %) currency))
       first
       :available))

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

(def probability-bets
  {0.1 1.0, 0.01 2.0, 0.001 4.0,
   0.9 1.0, 0.99 2.0, 0.999 4.0}
  )
(def narrow-probability-bets
  {0.25 1.0, 0.15 2.0, 0.1 4.0,
   0.75 1.0, 0.85 2.0, 0.9 4.0})

(def mostly-buy-probability-bets
  {0.1 0.2, 0.02 1.0, 0.005 2.0
   0.9 0.05, 0.98 0.2, 0.995 0.4})

(def timescales {60000 [0.002 0.0045] ;; with mu and sigma for lognormal dist
                 (* 10 60000) [0.004 0.012]
                 (* 100 60000) [0.016 0.032]
                 (* 1000 60000) [0.032 0.076]
                 (* 10000 60000) [0.086 0.19]})

(defn round-number
  "Round a double to the given precision (number of significant digits)"
  [factor p]
  (/ (Math/round (* p factor)) factor))

(def round-price (partial round-number 100.0))
(def round-bitcoin (partial round-number 10000000.0))
(defn trade-on-change-lifecycle
  "When the order is filled, call on-fill-hook with the feed item. If
   order isn't filled by the ttl, cancel the order."
  [price size buy? ttl-ms on-fill-hook match-pub]
  (a/go
    (try (let [order-id (place-order (limit-order buy? size price))
               fill-ch (a/chan)
               sub (a/sub match-pub [order-id "match"] fill-ch)]
           (println (format "%s %f at %f, expires in %d ms"
                            (if buy? "Buying" "Selling")
                            (float size)
                            (float price)
                            ttl-ms))
           
           (let [[feed-item _] (a/alts! [fill-ch (a/timeout ttl-ms)])]
             (if feed-item
               ;; filled, call on-fill-hook
               (do (when on-fill-hook
                    (on-fill-hook feed-item))
                   feed-item)
               ;; timed out, cancel
               (do (println "killing order" order-id)
                   (kill-order order-id)))))
        (catch Exception e
          (.printStackTrace e)))))

(def halt-trading (atom nil))

(defn trade-loop [min-bet probability-bets timescale overlap-factor match-pub shutdown-ch]
  (a/go-loop []
    (doseq [[probability bet-scale] (deref probability-bets)]
      (let [cur-price @current-price
            [mu sigma] (timescales timescale)
            exp-price (expected-price cur-price mu sigma probability)
            rounded-price (round-price exp-price)
            buy? (< exp-price cur-price)
            size (round-bitcoin (* min-bet bet-scale))]
        (a/<! (a/timeout 200))
        (trade-on-change-lifecycle rounded-price size buy? timescale nil match-pub)))
    (let [[v ch] (a/alts! [shutdown-ch (a/timeout (int (/ timescale overlap-factor)))])]
      (when-not (= ch shutdown-ch)
        (recur))))
  (kill-all-orders))

(comment
 (expected-price 580.0 0.004 0.012 0.005)
  (:startup (do




              (def *credentials* {:CB-ACCESS-KEY "7b5fe60c0f3d948984191ca4e32e60e6"
                                  :CB-ACCESS-PASSPHRASE "rju2hl21jwq"
                                  :CB-ACCESS-SECRET "U//grHPDjpr3VVGQCCm6A2ZffDVCT0zdkNNSzgcNe0Wh/HUBfp/jd0Sdk0G+JFJ2LEkMN5JByyGvo7ki0P9Njw=="})
              (defonce feed-client (create-feed-client ws-url))
              
              (def pubs (pub-sub (:out feed-client)))
              (let [ch (a/chan )]
                (a/sub (:by-type pubs) "match" ch)
                (keep-current-price-updated ch current-price))
              (let [result-ch (a/chan (a/sliding-buffer 100))]
                  (doseq [ts (keys timescales)]
                    (trade-loop 0.001 probability-bets ts 5)))
            ;; to halt
            (reset! halt-trading nil)
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

          (map #(Math/log %) [60 300 3600 21600 86400 608400]))
