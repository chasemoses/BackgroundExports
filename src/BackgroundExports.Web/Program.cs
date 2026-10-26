using System.Security.Claims;
using BackgroundExports.Core;
using BackgroundExports.Web;
using BackgroundExports.Web.Components;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddRazorComponents().AddInteractiveServerComponents();

// Cookie sign-in stands in for SSO. Pages and the download endpoint both read the same user,
// so the endpoint can check that you're downloading your own export.
builder.Services.AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme).AddCookie(o => o.LoginPath = "/demo-login");
builder.Services.AddAuthorization();
builder.Services.AddCascadingAuthenticationState();

builder.Services.AddDbContextFactory<ExportsDbContext>(o => o.UseSqlServer(builder.Configuration.GetConnectionString("Exports")));

var delay = TimeSpan.FromMilliseconds(builder.Configuration.GetValue("Exports:DelayPerChunkMs", 150));
builder.Services.AddSingleton<IExportGenerator>(new InventoryExportGenerator(delay));
builder.Services.AddSingleton<IExportGenerator>(new FlakyInventoryExportGenerator(delay));
builder.Services.AddSingleton(new ExportOptions
{
    OutputDirectory = Directory.CreateDirectory(Path.Combine(builder.Environment.ContentRootPath, "App_Data", "exports")).FullName,
});
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<JobSignal>();
builder.Services.AddSingleton<JobNotifier>();
builder.Services.AddSingleton<JobCancellation>();
builder.Services.AddSingleton<ExportQueue>();
builder.Services.AddSingleton<JobProcessor>();
builder.Services.AddHostedService<ExportWorker>();

var app = builder.Build();

if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Error", createScopeForErrors: true);
    app.UseHsts();
}

app.UseAuthentication();
app.UseAuthorization();
app.UseAntiforgery();
app.MapStaticAssets();

app.MapGet("/demo-login", async (HttpContext http, string? user, string? returnUrl) =>
{
    user ??= DemoUsers.All[0]; // anyone not signed in becomes the first demo user
    if (!DemoUsers.All.Contains(user)) return Results.BadRequest();
    var identity = new ClaimsIdentity([new Claim(ClaimTypes.Name, user)], CookieAuthenticationDefaults.AuthenticationScheme);
    await http.SignInAsync(new ClaimsPrincipal(identity));
    // Only same-site paths: "//evil.example" and "/\evil.example" are other sites to a browser.
    var local = returnUrl is { Length: > 0 } && returnUrl[0] == '/' && (returnUrl.Length == 1 || returnUrl[1] is not ('/' or '\\'));
    return Results.LocalRedirect(local ? returnUrl! : "/");
});

app.MapGet("/exports/{id:guid}/download", async (Guid id, ClaimsPrincipal user, ExportQueue queue, CancellationToken ct) =>
{
    var result = await queue.OpenResultAsync(id, user.Identity!.Name!, ct);
    return result is null
        ? Results.NotFound()
        : Results.File(result.Value.Content, "application/zip", result.Value.FileName, enableRangeProcessing: true);
}).RequireAuthorization();

app.MapRazorComponents<App>().AddInteractiveServerRenderMode().RequireAuthorization();

await using (var scope = app.Services.CreateAsyncScope())
{
    var db = await scope.ServiceProvider.GetRequiredService<IDbContextFactory<ExportsDbContext>>().CreateDbContextAsync();
    if (args.Contains("--reset")) await db.Database.EnsureDeletedAsync();
    await db.Database.EnsureCreatedAsync();
}

app.Run();

public static class DemoUsers
{
    public static readonly string[] All = ["ana.registrar", "ben.facilities", "cy.finance"];
}
