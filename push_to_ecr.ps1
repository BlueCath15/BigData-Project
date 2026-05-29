# push_to_ecr.ps1
# Construye la imagen Docker y la sube a ECR
# Ejecutar desde la raiz del proyecto: .\push_to_ecr.ps1

$ACCOUNT_ID = "992382522951"
$REGION     = "us-east-1"
$REPO_NAME  = "fraud-detection-fastapi"
$IMAGE_TAG  = "latest"

$ECR_URL = "$ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com"
$FULL_IMAGE = "$ECR_URL/$REPO_NAME`:$IMAGE_TAG"

Write-Host "[1/4] Autenticando en ECR..." -ForegroundColor Cyan
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ECR_URL

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error en autenticacion. Verifica tus credenciales AWS." -ForegroundColor Red
    exit 1
}

Write-Host "[2/4] Construyendo imagen Docker..." -ForegroundColor Cyan
docker build -t $REPO_NAME .

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error construyendo la imagen." -ForegroundColor Red
    exit 1
}

Write-Host "[3/4] Etiquetando imagen..." -ForegroundColor Cyan
docker tag "$REPO_NAME`:$IMAGE_TAG" $FULL_IMAGE

Write-Host "[4/4] Subiendo imagen a ECR..." -ForegroundColor Cyan
docker push $FULL_IMAGE

if ($LASTEXITCODE -ne 0) {
    Write-Host "Error subiendo la imagen." -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "Imagen subida exitosamente:" -ForegroundColor Green
Write-Host $FULL_IMAGE -ForegroundColor Green
Write-Host ""
Write-Host "Recuerda actualizar la task definition de ECS si ya existe." -ForegroundColor Yellow
