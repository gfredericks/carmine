(ns taoensso.carmine.tests.message-queue
  (:require [expectations     :as tests :refer :all]
            [taoensso.carmine :as car   :refer (wcar)]
            [taoensso.carmine.message-queue :as mq]))

(comment
  (remove-ns        'taoensso.carmine.tests.message-queue)
  (tests/run-tests '[taoensso.carmine.tests.message-queue]))

(def ^:private tq :carmine-test-queue)
(def ^:private conn-opts {})

(defn- clear-tq  [] (mq/clear-queues conn-opts tq))
(defn- tq-status [] (mq/queue-status conn-opts tq))
(defn- before-run {:expectations-options :before-run} [] (clear-tq))
(defn- after-run  {:expectations-options :after-run}  [] (clear-tq))

(defmacro wcar* [& body] `(car/wcar conn-opts ~@body))

(defn- dequeue* [qname & [opts]]
  (let [r (mq/dequeue qname (merge {:eoq-backoff-ms 175} opts))]
    (Thread/sleep 205) r))

;; (defmacro expect* [n e a]
;;   `(expect e (do (println (str ~n ":" (tq-status))) ~a)))

(expect (do (println (str "Running message queue tests")) true))

;;;; Basic enqueuing & dequeuing
(expect "eoq-backoff"    (do (clear-tq) (wcar* (dequeue* tq))))
(expect "mid1"           (wcar* (mq/enqueue tq :msg1 :mid1)))
(expect {:messages   {"mid1" :msg1},
         :mid-circle ["mid1" "end-of-circle"]} (in (tq-status)))
(expect :queued                     (wcar* (mq/message-status tq :mid1)))
(expect {:carmine.mq/error :queued} (wcar* (mq/enqueue tq :msg1 :mid1))) ; Dupe
(expect "eoq-backoff"    (wcar* (dequeue* tq)))
(expect ["mid1" :msg1 1] (wcar* (dequeue* tq))) ; New msg
(expect :locked          (wcar* (mq/message-status tq :mid1)))
(expect "eoq-backoff"    (wcar* (dequeue* tq)))
(expect nil              (wcar* (dequeue* tq))) ; Locked msg

;;;; Handling: success
(expect "mid1" (do (clear-tq) (wcar* (mq/enqueue tq :msg1 :mid1))))
;; (expect "eoq-backoff" (wcar* (dequeue* tq)))
;; Handler will *not* run against eoq-backoff/nil reply:
(expect nil (mq/handle1 conn-opts tq nil (wcar* (dequeue* tq))))
(expect {:mid "mid1" :message :msg1, :attempt 1}
  (let [p (promise)]
    (mq/handle1 conn-opts tq #(do (deliver p %) {:status :success})
      (wcar* (dequeue* tq)))
    @p))
(expect :done-awaiting-gc (wcar* (mq/message-status tq :mid1)))
(expect "eoq-backoff"     (wcar* (dequeue* tq)))
(expect nil               (wcar* (dequeue* tq))) ; Will gc
(expect nil               (wcar* (mq/message-status tq :mid1)))

;;;; Handling: handler crash
(expect "mid1"        (do (clear-tq) (wcar* (mq/enqueue tq :msg1 :mid1))))
(expect "eoq-backoff" (wcar* (dequeue* tq)))
(expect ["mid1" :msg1 1]
  (wcar* (dequeue* tq {:lock-ms 3000}))) ; Simulates bad handler
(expect :locked          (wcar* (mq/message-status tq :mid1)))
(expect "eoq-backoff"    (wcar* (dequeue* tq)))
(expect ["mid1" :msg1 2] (do (Thread/sleep 3000) ; Wait for lock to expire
                             (wcar* (dequeue* tq))))

;;;; Handling: retry with backoff
(expect "mid1"        (do (clear-tq) (wcar* (mq/enqueue tq :msg1 :mid1))))
(expect "eoq-backoff" (wcar* (dequeue* tq)))
(expect {:mid "mid1" :message :msg1, :attempt 1}
  (let [p (promise)]
    (mq/handle1 conn-opts tq #(do (deliver p %) {:status :retry :backoff-ms 3000})
      (wcar* (dequeue* tq)))
    @p))
(expect :queued-with-backoff (wcar* (mq/message-status tq :mid1)))
(expect "eoq-backoff"        (wcar* (dequeue* tq)))
(expect nil                  (wcar* (dequeue* tq))) ; Backoff (< 3s)
(expect "eoq-backoff"        (wcar* (dequeue* tq)))
(expect ["mid1" :msg1 2]     (do (Thread/sleep 3000) ; Wait for backoff to expire
                                 (wcar* (dequeue* tq))))

;;;; Handling: success with backoff (dedupe)
(expect "mid1"        (do (clear-tq) (wcar* (mq/enqueue tq :msg1 :mid1))))
(expect "eoq-backoff" (wcar* (dequeue* tq)))
(expect {:mid "mid1" :message :msg1, :attempt 1}
  (let [p (promise)]
    (mq/handle1 conn-opts tq #(do (deliver p %) {:status :success :backoff-ms 3000})
      (wcar* (dequeue* tq)))
    @p))
(expect :done-with-backoff (wcar* (mq/message-status tq :mid1)))
(expect "eoq-backoff"      (wcar* (dequeue* tq)))
(expect nil                (wcar* (dequeue* tq))) ; Will gc
(expect :done-with-backoff (wcar* (mq/message-status tq :mid1))) ; Backoff (< 3s)
(expect {:carmine.mq/error :done-with-backoff}
  (wcar* (mq/enqueue tq :msg1 :mid1))) ; Dupe
(expect "mid1" (do (Thread/sleep 3000) ; Wait for backoff to expire
                   (wcar* (mq/enqueue tq :msg1 :mid1))))

;;;; Handling: enqueue while :locked
(expect "mid1"        (do (clear-tq) (wcar* (mq/enqueue tq :msg1 :mid1))))
(expect "eoq-backoff" (wcar* (dequeue* tq)))
(expect :locked (do (future
                      (mq/handle1 conn-opts tq (fn [_] (Thread/sleep 3000) ; Hold lock
                                                 {:status :success})
                        (wcar* (dequeue* tq))))
                    (Thread/sleep 50)
                    (wcar* (mq/message-status tq :mid1))))
(expect {:carmine.mq/error :locked} (wcar* (mq/enqueue tq :msg1 :mid1)))
(expect "mid1" (wcar* (mq/enqueue tq :msg1 :mid1 :allow-requeue)))
(expect {:carmine.mq/error :locked-with-requeue}
  (wcar* (mq/enqueue tq :msg1-requeued :mid1 :allow-requeue)))
(expect :queued ; cmp :done-awaiting-gc
  (do (Thread/sleep 3500) ; Wait for handler to complete (extra time for future!)
      (wcar* (mq/message-status tq :mid1))))
(expect "eoq-backoff"    (wcar* (dequeue* tq)))
(expect ["mid1" :msg1 1] (wcar* (dequeue* tq)))

;;;; Handling: enqueue while :done-with-backoff
(expect "mid1" (do (clear-tq) (wcar* (mq/enqueue tq :msg1 :mid1))))
(expect "eoq-backoff" (wcar* (dequeue* tq)))
(expect :done-with-backoff
  (do (mq/handle1 conn-opts tq (fn [_] {:status :success :backoff-ms 3000})
        (wcar* (dequeue* tq)))
      (Thread/sleep 20)
      (wcar* (mq/message-status tq :mid1))))
(expect {:carmine.mq/error :done-with-backoff} (wcar* (mq/enqueue tq :msg1 :mid1)))
(expect "mid1" (wcar* (mq/enqueue tq :msg1-requeued :mid1 :allow-requeue)))
(expect :queued ; cmp :done-awaiting-gc
  (do (Thread/sleep 3000) ; Wait for backoff to expire
      (wcar* (mq/message-status tq :mid1))))
(expect "eoq-backoff"    (wcar* (dequeue* tq)))
(expect ["mid1" :msg1 1] (wcar* (dequeue* tq)))
