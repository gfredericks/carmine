(require '[taoensso.carmine.message-queue :as mq]
         '[taoensso.carmine :refer [wcar] :as car])

(def conn {:pool {:max-total-per-key (Long/parseLong (System/getenv "CARMINE_POOL_SIZE"))}})

^{:tag :scratch}
(wcar conn
      (mq/enqueue :my-queue :my-msg))

(let [p (promise)
      w
      (mq/worker conn :my-queue
                 {:handler (bound-fn [{:keys [message]}]
                             (println "Received" message)
                             (deliver p true)
                             {:status :success})})]
  (deref p)
  (mq/stop w))

(shutdown-agents)
