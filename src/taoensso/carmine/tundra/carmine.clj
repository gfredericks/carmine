(ns taoensso.carmine.tundra.carmine
  "Secondary Redis server DataStore implementation for Tundra."
  {:author "Peter Taoussanis"}
  (:require [taoensso.encore         :as encore]
            [taoensso.timbre         :as timbre]
            [taoensso.carmine.tundra :as tundra])
  (:import  [taoensso.carmine.tundra IDataStore]))

;; TODO

;; (defrecord CarmineDataStore [conn-opts]
;;   IDataStore
;;   (put-key    [dstore k v])
;;   (fetch-keys [dstore ks]))
