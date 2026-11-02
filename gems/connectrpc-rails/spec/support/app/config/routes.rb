# frozen_string_literal: true

Rails.application.routes.draw do
  match "/example.v1.EchoService(/*rpc)", to: "echo#connect", via: :all, format: false
  scope "/rpc" do
    match "/example.v1.EchoService(/*rpc)", to: "echo#connect", via: :all, format: false
  end
  match "/base/example.v1.EchoService(/*rpc)", to: "base_echo#connect", via: :all, format: false
  match "/rate/example.v1.EchoService(/*rpc)", to: "rate#connect", via: :all, format: false
  match "/rescue/example.v1.EchoService(/*rpc)", to: "rescue_echo#connect", via: :all, format: false
  match "/wrong/other.Service(/*rpc)", to: "echo#connect", via: :all, format: false
  match "/example.Array(/*rpc)", to: "array#connect", via: :all, format: false
  match "/example.Scalar(/*rpc)", to: "scalar#connect", via: :all, format: false
  post "/ordinary", to: "echo#ordinary"
  post "/base/ordinary", to: "base_echo#ordinary"
  get "/base/ordinary", to: "base_echo#ordinary"
end
