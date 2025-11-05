# ---- build stage ----
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

ENV DOTNET_NOLOGO=1 \
    DOTNET_CLI_TELEMETRY_OPTOUT=1 \
    NUGET_XMLDOC_MODE=skip

# 1) csproj だけ先にコピーしてウォームアップ用 restore（キャッシュ狙い）
COPY TimerSample.csproj ./
RUN dotnet nuget locals all --clear
RUN dotnet restore TimerSample.csproj -v:m --disable-parallel /p:RestoreUseStaticGraphEvaluation=false

# 2) 残りをコピー（ここでホストの bin/obj が混入しないように .dockerignore 必須）
COPY . .

# 3) 念のため bin/obj をクリーン（ホストから混入していた場合に備える）
RUN rm -rf bin obj

# 4) 本番用の restore（全ソース確定後にもう一度）
RUN dotnet restore TimerSample.csproj -v:m --disable-parallel /p:RestoreUseStaticGraphEvaluation=false

# 5) build / publish（no-restore で OK）
RUN dotnet build   TimerSample.csproj -c Release --no-restore -v:n  /p:RestoreUseStaticGraphEvaluation=false
RUN dotnet publish TimerSample.csproj -c Release --no-restore -o /app/publish /p:UseAppHost=false -v:n /p:RestoreUseStaticGraphEvaluation=false

# ---- runtime stage ----
FROM mcr.microsoft.com/azure-functions/dotnet-isolated:4-dotnet-isolated8.0
WORKDIR /home/site/wwwroot
COPY --from=build /app/publish .
ENV FUNCTIONS_WORKER_RUNTIME=dotnet-isolated \
    AZURE_FUNCTIONS_ENVIRONMENT=Development \
    ASPNETCORE_URLS=http://+:80
EXPOSE 80
